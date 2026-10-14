package service

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	goipam "github.com/metal-stack/go-ipam"
	v1 "github.com/metal-stack/go-ipam/api/v1"
	"github.com/metal-stack/go-ipam/api/v1/apiv1connect"
	"github.com/stretchr/testify/require"
)

// enteredGateStorage wraps storage and notifies when the delete hook fires,
// then blocks until the test releases the racing delete.
type enteredGateStorage struct {
	goipam.Storage
	entered chan<- struct{}
	proceed <-chan struct{}
	once    sync.Once
}

func (s *enteredGateStorage) DeletePrefix(ctx context.Context, prefix goipam.Prefix, namespace string) (goipam.Prefix, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.proceed
	return s.Storage.DeletePrefix(ctx, prefix, namespace)
}

func newTestServer(t *testing.T, log *slog.Logger, ipamer goipam.Ipamer, grpc bool) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(apiv1connect.NewIpamServiceHandler(New(log, ipamer)))
	server := httptest.NewUnstartedServer(mux)
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)
	return server
}

func newTestClient(server *httptest.Server, grpc bool) apiv1connect.IpamServiceClient {
	if grpc {
		return apiv1connect.NewIpamServiceClient(server.Client(), server.URL, connect.WithGRPC())
	}
	return apiv1connect.NewIpamServiceClient(server.Client(), server.URL)
}

func TestIpamService_DeletePrefixStatusCodes(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cases := []struct {
		name string
		grpc bool
	}{
		{name: "connect", grpc: false},
		{name: "grpc", grpc: true},
	}

	for _, tc := range cases {
		name, grpc := tc.name, tc.grpc

		// Fresh shared storage per protocol: three independent ipamer instances
		// as if they were three service processes.
		shared := goipam.NewMemory(ctx)
		setup := goipam.NewWithStorage(shared)
		acquirer := goipam.NewWithStorage(shared)
		deleter := goipam.NewWithStorage(shared)

		server := newTestServer(t, log, deleter, grpc)
		client := newTestClient(server, grpc)
		t.Run(name+"/notFound", func(t *testing.T) {
			_, err := client.DeletePrefix(ctx, &v1.DeletePrefixRequest{Cidr: "10.200.0.0/30"})
			require.Error(t, err)
			require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
		})

		t.Run(name+"/nonEmpty", func(t *testing.T) {
			cidr := "10.201.0.0/30"
			_, err := setup.NewPrefix(ctx, cidr)
			require.NoError(t, err)
			ip, err := acquirer.AcquireIP(ctx, cidr)
			require.NoError(t, err)
			require.NotNil(t, ip)

			_, err = client.DeletePrefix(ctx, &v1.DeletePrefixRequest{Cidr: cidr})
			require.Error(t, err)
			require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
		})

		t.Run(name+"/conflictAndRecovery", func(t *testing.T) {
			cidr := "10.202.0.0/30"
			_, err := setup.NewPrefix(ctx, cidr)
			require.NoError(t, err)

			// Rebuild the server-side ipamer with a gated storage for this
			// scenario: its delete blocks after the empty prefix was read.
			entered := make(chan struct{})
			proceed := make(chan struct{})
			gated := goipam.NewWithStorage(&enteredGateStorage{
				Storage: shared,
				entered: entered,
				proceed: proceed,
			})

			gatedServer := newTestServer(t, log, gated, grpc)
			gatedClient := newTestClient(gatedServer, grpc)

			deleteErr := make(chan error, 1)
			go func() {
				_, err := gatedClient.DeletePrefix(ctx, &v1.DeletePrefixRequest{Cidr: cidr})
				deleteErr <- err
			}()

			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("delete never reached the interleave point")
			}

			// A second instance allocates first.
			ip, err := acquirer.AcquireIP(ctx, cidr)
			require.NoError(t, err)
			require.NotNil(t, ip)

			close(proceed)
			select {
			case err := <-deleteErr:
				require.Error(t, err)
				require.Equal(t, connect.CodeAborted, connect.CodeOf(err), "conflict must be reported as Aborted")
			case <-time.After(5 * time.Second):
				t.Fatal("delete call never returned")
			}

			// Address survived; recovery: release, then delete succeeds.
			require.NoError(t, acquirer.ReleaseIPFromPrefix(ctx, cidr, ip.IP.String()))
			_, err = client.DeletePrefix(ctx, &v1.DeletePrefixRequest{Cidr: cidr})
			require.NoError(t, err)
		})

		t.Run(name+"/acquireOnDeletedPrefix", func(t *testing.T) {
			cidr := "10.203.0.0/30"
			_, err := setup.NewPrefix(ctx, cidr)
			require.NoError(t, err)
			_, err = deleter.DeletePrefix(ctx, cidr)
			require.NoError(t, err)

			_, err = client.AcquireIP(ctx, &v1.AcquireIPRequest{PrefixCidr: cidr})
			require.Error(t, err)
			require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

			// Explicitly recreate the prefix and retry.
			_, err = setup.NewPrefix(ctx, cidr)
			require.NoError(t, err)
			resp, err := client.AcquireIP(ctx, &v1.AcquireIPRequest{PrefixCidr: cidr})
			require.NoError(t, err)
			require.Equal(t, "10.203.0.1", resp.GetIp().GetIp())
		})
	}
}
