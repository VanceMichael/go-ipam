package ipam

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// staleReadStorage serves fixed stale snapshots from ReadPrefix while every other operation
// sees the live storage. It simulates another server process committing a record after this
// process read the prefix (eventual cross instance visibility).
type staleReadStorage struct {
	Storage
	stale map[string]Prefix
}

func (s *staleReadStorage) ReadPrefix(ctx context.Context, prefix string, namespace string) (Prefix, error) {
	if stale, ok := s.stale[prefix]; ok {
		return *stale.deepCopy(), nil
	}
	return s.Storage.ReadPrefix(ctx, prefix, namespace)
}

// flakyStorage wraps a Storage and can fail every UpdatePrefix on demand to simulate
// partial storage failures during idempotent acquisitions.
type flakyStorage struct {
	Storage
	failUpdate atomic.Bool
}

func (f *flakyStorage) UpdatePrefix(ctx context.Context, prefix Prefix, namespace string) (Prefix, error) {
	if f.failUpdate.Load() {
		return Prefix{}, errors.New("injected storage failure")
	}
	return f.Storage.UpdatePrefix(ctx, prefix, namespace)
}

// TestAcquireIPRequestID verifies the idempotent replay contract across every builtin storage:
// replays with the same client request id return the very same allocation, while calls without
// a request id keep the allocate-on-every-call semantics.
func TestAcquireIPRequestID(t *testing.T) {
	ctx := t.Context()
	testWithBackends(t, func(t *testing.T, ipam *ipamer) {
		cidr := "10.200.0.0/24"
		prefix, err := ipam.NewPrefix(ctx, cidr)
		require.NoError(t, err)
		require.NotNil(t, prefix)

		// first dynamic acquisition with a client request id
		ip1, err := ipam.AcquireIPWithRequestID(ctx, cidr, "client-request-1")
		require.NoError(t, err)
		require.Equal(t, "10.200.0.1", ip1.IP.String())

		// simulated lost response: replay returns the same address without allocating again
		ip1Replay, err := ipam.AcquireIPWithRequestID(ctx, cidr, "client-request-1")
		require.NoError(t, err)
		require.Equal(t, ip1.IP.String(), ip1Replay.IP.String())
		require.Equal(t, ip1.ParentPrefix, ip1Replay.ParentPrefix)

		// calls without a request id keep the existing per-call allocation semantics
		ip2, err := ipam.AcquireIP(ctx, cidr)
		require.NoError(t, err)
		require.Equal(t, "10.200.0.2", ip2.IP.String())

		// specific acquisition with a client request id
		ip3, err := ipam.AcquireSpecificIPWithRequestID(ctx, cidr, "10.200.0.99", "client-request-2")
		require.NoError(t, err)
		require.Equal(t, "10.200.0.99", ip3.IP.String())
		ip3Replay, err := ipam.AcquireSpecificIPWithRequestID(ctx, cidr, "10.200.0.99", "client-request-2")
		require.NoError(t, err)
		require.Equal(t, ip3.IP.String(), ip3Replay.IP.String())

		// the replay must not allocate the address twice: a non idempotent acquire of the
		// same address now reports it as already allocated
		_, err = ipam.AcquireSpecificIP(ctx, cidr, "10.200.0.99")
		require.ErrorIs(t, err, ErrAlreadyAllocated)

		// reserved network/broadcast (.0,.255) plus .1,.2,.99 are occupied
		reread, err := ipam.PrefixFrom(ctx, cidr)
		require.NoError(t, err)
		require.Len(t, reread.ips, 5)
		require.Len(t, reread.ipRequests, 2)

		// empty request ids are rejected on the idempotent entry points
		_, err = ipam.AcquireIPWithRequestID(ctx, cidr, "")
		require.Error(t, err)
		_, err = ipam.AcquireSpecificIPWithRequestID(ctx, cidr, "10.200.0.50", "")
		require.Error(t, err)
	})
}

// TestAcquireIPRequestIDConflict verifies that reusing a request id with a different operation
// kind, different specific address or a different prefix is explicitly rejected and allocates
// nothing.
func TestAcquireIPRequestIDConflict(t *testing.T) {
	ctx := t.Context()
	testWithBackends(t, func(t *testing.T, ipam *ipamer) {
		cidr := "10.201.0.0/24"
		otherCidr := "10.202.0.0/24"
		_, err := ipam.NewPrefix(ctx, cidr)
		require.NoError(t, err)
		_, err = ipam.NewPrefix(ctx, otherCidr)
		require.NoError(t, err)

		// dynamic then specific with the same id -> operation kind conflict
		ip, err := ipam.AcquireIPWithRequestID(ctx, cidr, "conflict-1")
		require.NoError(t, err)
		require.Equal(t, "10.201.0.1", ip.IP.String())
		_, err = ipam.AcquireSpecificIPWithRequestID(ctx, cidr, "10.201.0.5", "conflict-1")
		require.ErrorIs(t, err, ErrRequestIDConflict)

		// specific then dynamic with the same id -> operation kind conflict
		_, err = ipam.AcquireSpecificIPWithRequestID(ctx, cidr, "10.201.0.10", "conflict-2")
		require.NoError(t, err)
		_, err = ipam.AcquireIPWithRequestID(ctx, cidr, "conflict-2")
		require.ErrorIs(t, err, ErrRequestIDConflict)

		// specific .11 then specific .12 with the same id -> parameter conflict
		_, err = ipam.AcquireSpecificIPWithRequestID(ctx, cidr, "10.201.0.11", "conflict-3")
		require.NoError(t, err)
		_, err = ipam.AcquireSpecificIPWithRequestID(ctx, cidr, "10.201.0.12", "conflict-3")
		require.ErrorIs(t, err, ErrRequestIDConflict)

		// same id against a different prefix -> prefix conflict
		_, err = ipam.AcquireIPWithRequestID(ctx, otherCidr, "conflict-1")
		require.ErrorIs(t, err, ErrRequestIDConflict)
		_, err = ipam.AcquireSpecificIPWithRequestID(ctx, otherCidr, "10.202.0.9", "conflict-2")
		require.ErrorIs(t, err, ErrRequestIDConflict)

		// conflicts never occupy addresses: .5,.12 and the whole other prefix are untouched,
		// successful allocations are .1 and .10,.11
		reread, err := ipam.PrefixFrom(ctx, cidr)
		require.NoError(t, err)
		require.Contains(t, reread.ips, "10.201.0.1")
		require.Contains(t, reread.ips, "10.201.0.10")
		require.Contains(t, reread.ips, "10.201.0.11")
		require.NotContains(t, reread.ips, "10.201.0.5")
		require.NotContains(t, reread.ips, "10.201.0.12")
		require.Len(t, reread.ips, 5)

		other, err := ipam.PrefixFrom(ctx, otherCidr)
		require.NoError(t, err)
		require.Len(t, other.ips, 2)
		require.Empty(t, other.ipRequests)
	})
}

// TestAcquireIPRequestIDConcurrent fires many concurrent replays with the same request id
// and concurrent non identified acquisitions: exactly one address is produced for the
// identified request, all replays return it.
func TestAcquireIPRequestIDConcurrent(t *testing.T) {
	ctx := t.Context()
	testWithBackends(t, func(t *testing.T, ipam *ipamer) {
		cidr := "10.203.0.0/24"
		_, err := ipam.NewPrefix(ctx, cidr)
		require.NoError(t, err)

		const replayClients = 32
		var wg sync.WaitGroup
		start := make(chan struct{})
		replayIPs := make([]string, replayClients)
		replayErrs := make([]error, replayClients)
		for i := range replayClients {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				ip, err := ipam.AcquireIPWithRequestID(ctx, cidr, "concurrent-request")
				replayErrs[i] = err
				if err == nil {
					replayIPs[i] = ip.IP.String()
				}
			}(i)
		}
		close(start)
		wg.Wait()

		for _, err := range replayErrs {
			require.NoError(t, err)
		}
		for _, got := range replayIPs {
			require.Equal(t, "10.203.0.1", got)
		}

		// concurrent acquisitions without request id still each get their own address
		const plainClients = 16
		plainIPs := make([]string, plainClients)
		plainErrs := make([]error, plainClients)
		start = make(chan struct{})
		for i := range plainClients {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				ip, err := ipam.AcquireIP(ctx, cidr)
				plainErrs[i] = err
				if err == nil {
					plainIPs[i] = ip.IP.String()
				}
			}(i)
		}
		close(start)
		wg.Wait()
		for _, err := range plainErrs {
			require.NoError(t, err)
		}
		seen := map[string]bool{}
		for _, got := range plainIPs {
			require.NotEqual(t, "10.203.0.1", got)
			require.False(t, seen[got], "plain acquire returned duplicated ip:%s", got)
			seen[got] = true
		}
		require.Len(t, seen, plainClients)

		// concurrent replays of a specific acquisition all return the same address
		const specificClients = 24
		specificIPs := make([]string, specificClients)
		specificErrs := make([]error, specificClients)
		start = make(chan struct{})
		for i := range specificClients {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				ip, err := ipam.AcquireSpecificIPWithRequestID(ctx, cidr, "10.203.0.50", "concurrent-specific-request")
				specificErrs[i] = err
				if err == nil {
					specificIPs[i] = ip.IP.String()
				}
			}(i)
		}
		close(start)
		wg.Wait()
		for _, err := range specificErrs {
			require.NoError(t, err)
		}
		for _, got := range specificIPs {
			require.Equal(t, "10.203.0.50", got)
		}

		// reserved(.0,.255) + 1 identified dynamic + 16 plain + 1 identified specific
		reread, err := ipam.PrefixFrom(ctx, cidr)
		require.NoError(t, err)
		require.Len(t, reread.ips, 2+1+plainClients+1)
		require.Len(t, reread.ipRequests, 2)
	})
}

// TestAcquireIPRequestIDCrossInstanceReplay verifies that a record committed by another
// process after the prefix was read is found via the namespace scan and replayed directly,
// instead of colliding on the occupied address or losing the optimistic lock.
func TestAcquireIPRequestIDCrossInstanceReplay(t *testing.T) {
	ctx := t.Context()
	testWithBackends(t, func(t *testing.T, ipam *ipamer) {
		cidr := "10.207.0.0/24"
		_, err := ipam.NewPrefix(ctx, cidr)
		require.NoError(t, err)

		// snapshot before the other process commits its idempotent allocation
		before, err := ipam.PrefixFrom(ctx, cidr)
		require.NoError(t, err)
		require.Empty(t, before.ipRequests)

		_, err = ipam.AcquireSpecificIPWithRequestID(ctx, cidr, "10.207.0.33", "cross-instance")
		require.NoError(t, err)

		// this ipamer keeps reading the stale prefix but scans the live namespace
		stale := &staleReadStorage{
			Storage: ipam.storage,
			stale:   map[string]Prefix{before.Cidr: *before},
		}
		lateJoiner := &ipamer{storage: stale}

		replay, err := lateJoiner.AcquireSpecificIPWithRequestID(ctx, cidr, "10.207.0.33", "cross-instance")
		require.NoError(t, err)
		require.Equal(t, "10.207.0.33", replay.IP.String())

		_, err = lateJoiner.AcquireSpecificIPWithRequestID(ctx, cidr, "10.207.0.34", "cross-instance")
		require.ErrorIs(t, err, ErrRequestIDConflict)
	})
}

// TestAcquireIPRequestIDStorageFailure makes the prefix write fail and asserts that no
// address is occupied without a replayable record: a retry after recovery with the same
// request id performs the single allocation, later replays return it.
func TestAcquireIPRequestIDStorageFailure(t *testing.T) {
	ctx := t.Context()
	testWithBackends(t, func(t *testing.T, ipam *ipamer) {
		underlying := ipam.storage
		flaky := &flakyStorage{Storage: underlying}
		ipam.storage = flaky

		cidr := "10.204.0.0/24"
		_, err := ipam.NewPrefix(ctx, cidr)
		require.NoError(t, err)

		// every persistence attempt fails: the client gets an error ...
		flaky.failUpdate.Store(true)
		_, err = ipam.AcquireIPWithRequestID(ctx, cidr, "failed-request")
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrRequestIDConflict)

		// ... but the address is neither occupied nor the request recorded, so the same
		// request id remains replayable once storage recovers
		stored, err := underlying.ReadPrefix(ctx, cidr, defaultNamespace)
		require.NoError(t, err)
		require.Len(t, stored.ips, 2)
		require.Empty(t, stored.ipRequests)

		flaky.failUpdate.Store(false)
		ip, err := ipam.AcquireIPWithRequestID(ctx, cidr, "failed-request")
		require.NoError(t, err)
		require.Equal(t, "10.204.0.1", ip.IP.String())

		replay, err := ipam.AcquireIPWithRequestID(ctx, cidr, "failed-request")
		require.NoError(t, err)
		require.Equal(t, ip.IP.String(), replay.IP.String())

		reread, err := underlying.ReadPrefix(ctx, cidr, defaultNamespace)
		require.NoError(t, err)
		require.Len(t, reread.ips, 3)
		require.Len(t, reread.ipRequests, 1)
		require.Equal(t, "10.204.0.1", reread.ipRequests["failed-request"].IP)
	})
}

// TestAcquireIPRequestIDRestart verifies the allocation is durable across service restarts
// (a fresh ipamer instance, and for the file backend a cold storage reload from disk).
func TestAcquireIPRequestIDRestart(t *testing.T) {
	ctx := t.Context()
	testWithBackends(t, func(t *testing.T, ipam *ipamer) {
		storage := ipam.storage
		cidr := "10.205.0.0/24"
		_, err := ipam.NewPrefix(ctx, cidr)
		require.NoError(t, err)

		dynamic, err := ipam.AcquireIPWithRequestID(ctx, cidr, "restart-dynamic")
		require.NoError(t, err)
		require.Equal(t, "10.205.0.1", dynamic.IP.String())
		specific, err := ipam.AcquireSpecificIPWithRequestID(ctx, cidr, "10.205.0.77", "restart-specific")
		require.NoError(t, err)
		require.Equal(t, "10.205.0.77", specific.IP.String())

		// the records live in the shared storage, not in the ipamer process
		stored, err := storage.ReadPrefix(ctx, cidr, defaultNamespace)
		require.NoError(t, err)
		require.Contains(t, stored.ipRequests, "restart-dynamic")
		require.Contains(t, stored.ipRequests, "restart-specific")

		// simulate a service restart: brand new ipamer on top of the same storage
		restarted := NewWithStorage(storage)
		replayDynamic, err := restarted.AcquireIPWithRequestID(ctx, cidr, "restart-dynamic")
		require.NoError(t, err)
		require.Equal(t, dynamic.IP.String(), replayDynamic.IP.String())
		replaySpecific, err := restarted.AcquireSpecificIPWithRequestID(ctx, cidr, "10.205.0.77", "restart-specific")
		require.NoError(t, err)
		require.Equal(t, specific.IP.String(), replaySpecific.IP.String())
		_, err = restarted.AcquireSpecificIPWithRequestID(ctx, cidr, "10.205.0.78", "restart-specific")
		require.ErrorIs(t, err, ErrRequestIDConflict)

		// cold restart of the file backend from the state file on disk
		if f, ok := storage.(*file); ok {
			cold := NewWithStorage(NewLocalFile(ctx, f.path))
			coldDynamic, err := cold.AcquireIPWithRequestID(ctx, cidr, "restart-dynamic")
			require.NoError(t, err)
			require.Equal(t, dynamic.IP.String(), coldDynamic.IP.String())
			coldSpecific, err := cold.AcquireSpecificIPWithRequestID(ctx, cidr, "10.205.0.77", "restart-specific")
			require.NoError(t, err)
			require.Equal(t, specific.IP.String(), coldSpecific.IP.String())
		}
	})
}

// TestAcquireIPRequestIDReleaseFinalizesRecord verifies that releasing the address, deleting
// the prefix or deleting the namespace also finalize the idempotency records, so later
// legitimate requests allocate fresh results instead of hitting a stale allocation.
func TestAcquireIPRequestIDReleaseFinalizesRecord(t *testing.T) {
	ctx := t.Context()
	testWithBackends(t, func(t *testing.T, ipam *ipamer) {
		cidr := "10.206.0.0/24"
		_, err := ipam.NewPrefix(ctx, cidr)
		require.NoError(t, err)

		ip, err := ipam.AcquireIPWithRequestID(ctx, cidr, "reusable-request")
		require.NoError(t, err)
		require.Equal(t, "10.206.0.1", ip.IP.String())

		// release the allocated address: the idempotency record is finalized with it
		err = ipam.ReleaseIPFromPrefix(ctx, cidr, ip.IP.String())
		require.NoError(t, err)
		reread, err := ipam.PrefixFrom(ctx, cidr)
		require.NoError(t, err)
		require.NotContains(t, reread.ips, "10.206.0.1")
		require.Empty(t, reread.ipRequests)

		// reusing the request id after release allocates a fresh result instead of
		// returning the stale address
		again, err := ipam.AcquireIPWithRequestID(ctx, cidr, "reusable-request")
		require.NoError(t, err)
		require.Equal(t, "10.206.0.1", again.IP.String())
		fresh, err := ipam.PrefixFrom(ctx, cidr)
		require.NoError(t, err)
		require.Contains(t, fresh.ipRequests, "reusable-request")

		// after release the same request id may bind to different parameters
		err = ipam.ReleaseIPFromPrefix(ctx, cidr, again.IP.String())
		require.NoError(t, err)
		specific, err := ipam.AcquireSpecificIPWithRequestID(ctx, cidr, "10.206.0.50", "reusable-request")
		require.NoError(t, err)
		require.Equal(t, "10.206.0.50", specific.IP.String())
		err = ipam.ReleaseIPFromPrefix(ctx, cidr, specific.IP.String())
		require.NoError(t, err)

		// deleting the prefix removes all remaining records with it
		_, err = ipam.DeletePrefix(ctx, cidr)
		require.NoError(t, err)
		_, err = ipam.NewPrefix(ctx, cidr)
		require.NoError(t, err)
		recreated, err := ipam.AcquireIPWithRequestID(ctx, cidr, "reusable-request")
		require.NoError(t, err)
		require.Equal(t, "10.206.0.1", recreated.IP.String())
		err = ipam.ReleaseIPFromPrefix(ctx, cidr, recreated.IP.String())
		require.NoError(t, err)
		_, err = ipam.DeletePrefix(ctx, cidr)
		require.NoError(t, err)

		// deleting a namespace finalizes all records of that namespace
		namespace := "idempotency-ns"
		require.NoError(t, ipam.CreateNamespace(ctx, namespace))
		nsCtx := NewContextWithNamespace(ctx, namespace)
		_, err = ipam.NewPrefix(nsCtx, cidr)
		require.NoError(t, err)
		nsIP, err := ipam.AcquireIPWithRequestID(nsCtx, cidr, "namespaced-request")
		require.NoError(t, err)
		require.Equal(t, "10.206.0.1", nsIP.IP.String())

		err = ipam.ReleaseIPFromPrefix(nsCtx, cidr, nsIP.IP.String())
		require.NoError(t, err)
		_, err = ipam.DeletePrefix(nsCtx, cidr)
		require.NoError(t, err)
		require.NoError(t, ipam.DeleteNamespace(ctx, namespace))

		// recreate namespace and prefix: the old request id must not hit a stale allocation
		require.NoError(t, ipam.CreateNamespace(ctx, namespace))
		nsCtx = NewContextWithNamespace(ctx, namespace)
		_, err = ipam.NewPrefix(nsCtx, cidr)
		require.NoError(t, err)
		againNS, err := ipam.AcquireIPWithRequestID(nsCtx, cidr, "namespaced-request")
		require.NoError(t, err)
		require.Equal(t, "10.206.0.1", againNS.IP.String())

		// clean up the namespace so it does not leak into other tests on shared storage
		err = ipam.ReleaseIPFromPrefix(nsCtx, cidr, againNS.IP.String())
		require.NoError(t, err)
		_, err = ipam.DeletePrefix(nsCtx, cidr)
		require.NoError(t, err)
		require.NoError(t, ipam.DeleteNamespace(ctx, namespace))
	})
}
