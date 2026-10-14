package service

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"connectrpc.com/connect"
	goipam "github.com/metal-stack/go-ipam"
	v1 "github.com/metal-stack/go-ipam/api/v1"
	"github.com/metal-stack/go-ipam/api/v1/apiv1connect"
	"github.com/stretchr/testify/require"
)

func newNamespaceTestClient(t *testing.T, ctx context.Context) apiv1connect.IpamServiceClient {
	t.Helper()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	mux := http.NewServeMux()
	mux.Handle(apiv1connect.NewIpamServiceHandler(
		New(log, goipam.New(ctx)),
	))
	server := httptest.NewUnstartedServer(mux)
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)
	return apiv1connect.NewIpamServiceClient(server.Client(), server.URL)
}

// TestIpamService_NamespaceLifecycle verifies the stable connect error codes
// of the lifecycle transition: non-empty -> FailedPrecondition, missing ->
// NotFound, and creation after a committed delete -> NotFound until the
// namespace is recreated.
func TestIpamService_NamespaceLifecycle(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client := newNamespaceTestClient(t, ctx)

	namespace := "svc-lifecycle"
	ns := namespace

	_, err := client.CreateNamespace(ctx, &v1.CreateNamespaceRequest{Namespace: namespace})
	require.NoError(t, err)

	// First prefix commits: delete must be FailedPrecondition.
	_, err = client.CreatePrefix(ctx, &v1.CreatePrefixRequest{Cidr: "10.99.0.0/24", Namespace: &ns})
	require.NoError(t, err)

	_, err = client.DeleteNamespace(ctx, &v1.DeleteNamespaceRequest{Namespace: namespace})
	require.Error(t, err)
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

	// Remove the prefix, delete the namespace.
	_, err = client.DeletePrefix(ctx, &v1.DeletePrefixRequest{Cidr: "10.99.0.0/24", Namespace: &ns})
	require.NoError(t, err)
	_, err = client.DeleteNamespace(ctx, &v1.DeleteNamespaceRequest{Namespace: namespace})
	require.NoError(t, err)

	// Second delete is NotFound.
	_, err = client.DeleteNamespace(ctx, &v1.DeleteNamespaceRequest{Namespace: namespace})
	require.Error(t, err)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// Prefix creation after the committed delete is NotFound.
	_, err = client.CreatePrefix(ctx, &v1.CreatePrefixRequest{Cidr: "10.99.0.0/24", Namespace: &ns})
	require.Error(t, err)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// Rebuild explicitly: creation succeeds again.
	_, err = client.CreateNamespace(ctx, &v1.CreateNamespaceRequest{Namespace: namespace})
	require.NoError(t, err)
	_, err = client.CreatePrefix(ctx, &v1.CreatePrefixRequest{Cidr: "10.99.0.0/24", Namespace: &ns})
	require.NoError(t, err)
}
