package ipam

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// lifecyclePair models two INDEPENDENT instances operating on the same
// backend: they have separate local namespace caches and, for remote
// backends, separate client connections. reopen constructs yet another fresh
// instance, simulating a client reconstruction / process restart.
type lifecyclePair struct {
	name    string
	a       Storage
	b       Storage
	reopen  func(Storage) (Storage, error)
	dispose func()
}

func peerStorage(ctx context.Context, s Storage) (Storage, error) {
	switch backend := s.(type) {
	case *memory:
		// In-process backend: another instance shares the same guarded state.
		return backend, nil
	case *file:
		return newFile(ctx, backend.path)
	case *kvStorage:
		host, port, err := net.SplitHostPort(backend.rdb.Options().Addr)
		if err != nil {
			return nil, err
		}
		return newRedis(ctx, host, port)
	case *kvEtcdStorage:
		host, port, err := net.SplitHostPort(backend.etcdDB.Endpoints()[0])
		if err != nil {
			return nil, err
		}
		return newEtcd(ctx, host, port, nil, nil, true)
	case *docStorage:
		client := backend.db.Client()
		return &mongodb{
			db:         client.Database(backend.db.Name()),
			sys:        client.Database(backend.sys.Name()),
			namespaces: make(map[string]struct{}),
		}, nil
	case *extendedSQL:
		return &sql{
			db:          backend.db,
			maxIdLength: backend.maxIdLength,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported storage type for independent instance: %T", s)
	}
}

func lifecyclePairs(ctx context.Context) []lifecyclePair {
	var pairs []lifecyclePair
	for _, provider := range storageProviders(ctx) {
		if backend != "" && backend != provider.name {
			continue
		}
		first := provider.provide()
		second, err := peerStorage(ctx, first)
		if err != nil {
			panic(fmt.Sprintf("unable to build independent %s instance: %s", provider.name, err))
		}
		f := first
		pairs = append(pairs, lifecyclePair{
			name: provider.name,
			a:    first,
			b:    second,
			reopen: func(Storage) (Storage, error) {
				return peerStorage(ctx, f)
			},
			dispose: func() {
				if tp, ok := first.(postCleanable); ok {
					_ = tp.postCleanup(ctx)
				}
			},
		})
	}
	return pairs
}

func assertNamespaceListed(t *testing.T, ctx context.Context, s Storage, namespace string, listed bool) {
	t.Helper()
	namespaces, err := s.ListNamespaces(ctx)
	require.NoError(t, err)
	var found bool
	for _, n := range namespaces {
		if n == namespace {
			found = true
		}
	}
	require.Equal(t, listed, found, "namespaces: %v", namespaces)
}

// TestNamespaceLifecycle_IndependentInstances exercises the two possible
// linearizable orderings of a first-prefix creation and an empty-namespace
// deletion through INDEPENDENT instances, and verifies that reconstructed
// clients (empty local caches) observe the same result.
func TestNamespaceLifecycle_IndependentInstances(t *testing.T) {
	ctx := t.Context()

	for _, pair := range lifecyclePairs(ctx) {
		t.Run(pair.name+"/create-commits-first", func(t *testing.T) {
			const namespace = "lc-create-wins"
			const cidr = "10.42.0.0/24"

			require.NoError(t, pair.a.CreateNamespace(ctx, namespace))
			t.Cleanup(func() {
				_, _ = pair.a.DeletePrefix(ctx, Prefix{Cidr: cidr}, namespace)
				_ = pair.a.DeleteNamespace(ctx, namespace)
			})

			// The first prefix of the namespace is created by instance B.
			_, err := pair.b.CreatePrefix(ctx, Prefix{Cidr: cidr}, namespace)
			require.NoError(t, err)

			// Instance A's concurrently possible cleanup must now lose: the
			// emptiness decision is atomic, so the delete is rejected and
			// the prefix is retained.
			err = pair.a.DeleteNamespace(ctx, namespace)
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrNamespaceNotEmpty), "expected ErrNamespaceNotEmpty, got: %v", err)
			require.Contains(t, err.Error(), "cannot delete namespace with allocated prefixes")

			// The new prefix is visible on both instances and survives.
			gotA, err := pair.a.ReadPrefix(ctx, cidr, namespace)
			require.NoError(t, err)
			require.Equal(t, cidr, gotA.Cidr)
			gotB, err := pair.b.ReadPrefix(ctx, cidr, namespace)
			require.NoError(t, err)
			require.Equal(t, cidr, gotB.Cidr)

			// A reconstructed client sees the retained state.
			fresh, err := pair.reopen(nil)
			require.NoError(t, err)
			assertNamespaceListed(t, ctx, fresh, namespace, true)
			gotFresh, err := fresh.ReadPrefix(ctx, cidr, namespace)
			require.NoError(t, err)
			require.Equal(t, cidr, gotFresh.Cidr)

			// The namespace is still healthy for further operations.
			cidrs, err := pair.a.ReadAllPrefixCidrs(ctx, namespace)
			require.NoError(t, err)
			require.Equal(t, []string{cidr}, cidrs)
		})

		t.Run(pair.name+"/delete-commits-first", func(t *testing.T) {
			const namespace = "lc-delete-wins"
			const cidr = "10.43.0.0/24"

			require.NoError(t, pair.b.CreateNamespace(ctx, namespace))

			// Instance A deletes the still-empty namespace.
			require.NoError(t, pair.a.DeleteNamespace(ctx, namespace))

			// Deleting again is not idempotent.
			err := pair.a.DeleteNamespace(ctx, namespace)
			require.ErrorIs(t, err, ErrNamespaceDoesNotExist)

			// Instance B's creation must fail with namespace-not-exist even
			// though its local cache may still contain a positive entry.
			_, err = pair.b.CreatePrefix(ctx, Prefix{Cidr: cidr}, namespace)
			require.ErrorIs(t, err, ErrNamespaceDoesNotExist)

			assertNamespaceListed(t, ctx, pair.a, namespace, false)
			assertNamespaceListed(t, ctx, pair.b, namespace, false)

			// Reconstructed / restarted client observes the same result.
			fresh, err := pair.reopen(nil)
			require.NoError(t, err)
			assertNamespaceListed(t, ctx, fresh, namespace, false)
			_, err = fresh.CreatePrefix(ctx, Prefix{Cidr: cidr}, namespace)
			require.ErrorIs(t, err, ErrNamespaceDoesNotExist)

			// Explicit rebuild makes creation retryable.
			require.NoError(t, pair.b.CreateNamespace(ctx, namespace))
			t.Cleanup(func() {
				_, _ = pair.b.DeletePrefix(ctx, Prefix{Cidr: cidr}, namespace)
				_ = pair.b.DeleteNamespace(ctx, namespace)
			})
			created, err := pair.b.CreatePrefix(ctx, Prefix{Cidr: cidr}, namespace)
			require.NoError(t, err)
			require.Equal(t, cidr, created.Cidr)

			// The rebuild and the prefix are observable by the other instance
			// and by yet another fresh client.
			gotA, err := pair.a.ReadPrefix(ctx, cidr, namespace)
			require.NoError(t, err)
			require.Equal(t, cidr, gotA.Cidr)
			fresh2, err := pair.reopen(nil)
			require.NoError(t, err)
			assertNamespaceListed(t, ctx, fresh2, namespace, true)
			gotFresh, err := fresh2.ReadPrefix(ctx, cidr, namespace)
			require.NoError(t, err)
			require.Equal(t, cidr, gotFresh.Cidr)

			// Now that the prefix exists, deletion is rejected again.
			err = pair.a.DeleteNamespace(ctx, namespace)
			require.ErrorIs(t, err, ErrNamespaceNotEmpty)
		})

		pair.dispose()
	}
}

// TestNamespaceLifecycle_Contract covers the ordered storage contract on
// every backend: error identity on missing/non-empty namespaces, root
// namespace compatibility and state after a canceled (timed-out) request.
func TestNamespaceLifecycle_Contract(t *testing.T) {
	ctx := t.Context()

	testWithBackends(t, func(t *testing.T, ipam *ipamer) {
		s := ipam.storage
		const namespace = "lc-contract"

		// Missing namespace: delete and create fail deterministically.
		err := s.DeleteNamespace(ctx, namespace)
		require.ErrorIs(t, err, ErrNamespaceDoesNotExist)
		_, err = s.CreatePrefix(ctx, Prefix{Cidr: "10.44.0.0/24"}, namespace)
		require.ErrorIs(t, err, ErrNamespaceDoesNotExist)

		// Empty namespace: delete succeeds, then it is gone.
		require.NoError(t, s.CreateNamespace(ctx, namespace))
		require.NoError(t, s.DeleteNamespace(ctx, namespace))
		assertNamespaceListed(t, ctx, s, namespace, false)
		_, err = s.ReadAllPrefixes(ctx, namespace)
		require.ErrorIs(t, err, ErrNamespaceDoesNotExist)

		// Rebuild: healthy operations on the recreated namespace.
		require.NoError(t, s.CreateNamespace(ctx, namespace))
		created, err := s.CreatePrefix(ctx, Prefix{Cidr: "10.44.0.0/24"}, namespace)
		require.NoError(t, err)
		require.Equal(t, "10.44.0.0/24", created.Cidr)
		err = s.DeleteNamespace(ctx, namespace)
		require.ErrorIs(t, err, ErrNamespaceNotEmpty)

		// Cleanup for the next backend run.
		_, err = s.DeletePrefix(ctx, created, namespace)
		require.NoError(t, err)
		require.NoError(t, s.DeleteNamespace(ctx, namespace))

		// Root namespace compatibility: non-empty root is rejected exactly
		// like tenant namespaces.
		rootPrefix, err := s.CreatePrefix(ctx, Prefix{Cidr: "10.45.0.0/24"}, defaultNamespace)
		require.NoError(t, err)
		err = s.DeleteNamespace(ctx, defaultNamespace)
		require.ErrorIs(t, err, ErrNamespaceNotEmpty)
		_, err = s.ReadPrefix(ctx, rootPrefix.Cidr, defaultNamespace)
		require.NoError(t, err)
		_, err = s.DeletePrefix(ctx, rootPrefix, defaultNamespace)
		require.NoError(t, err)

		// A canceled/timed-out request must not move the lifecycle: after the
		// failed call the empty namespace still exists and works. The
		// in-process memory and file backends do not honor context
		// cancellation (they never did).
		if s.Name() != "memory" && s.Name() != "file" {
			require.NoError(t, s.CreateNamespace(ctx, namespace))
			canceledCtx, cancel := context.WithCancel(ctx)
			cancel()
			err = s.DeleteNamespace(canceledCtx, namespace)
			require.Error(t, err, "canceled delete must return an error")
			assertNamespaceListed(t, ctx, s, namespace, true)
			healthy, err := s.CreatePrefix(ctx, Prefix{Cidr: "10.46.0.0/24"}, namespace)
			require.NoError(t, err)
			_, err = s.DeletePrefix(ctx, healthy, namespace)
			require.NoError(t, err)
			require.NoError(t, s.DeleteNamespace(ctx, namespace))
		}
	})
}

// TestNamespaceLifecycle_NeverBothSucceed hammers create and delete
// concurrently from independent instances: regardless of ordering, exactly
// one side may succeed and a reconstructed client must agree with the result.
func TestNamespaceLifecycle_NeverBothSucceed(t *testing.T) {
	ctx := t.Context()

	const rounds = 40

	for _, pair := range lifecyclePairs(ctx) {
		t.Run(pair.name, func(t *testing.T) {
			// Third independent instance with a cold cache used as witness.
			witness, err := pair.reopen(nil)
			require.NoError(t, err)

			for round := 0; round < rounds; round++ {
				namespace := fmt.Sprintf("lc-race-%d", round)
				cidr := fmt.Sprintf("10.%d.0.0/24", 100+round)
				require.NoError(t, pair.a.CreateNamespace(ctx, namespace))

				start := make(chan struct{})
				var (
					wg        sync.WaitGroup
					createOK  bool
					deleteOK  bool
					createErr error
					deleteErr error
				)
				wg.Add(2)
				go func() {
					defer wg.Done()
					<-start
					_, createErr = pair.b.CreatePrefix(ctx, Prefix{Cidr: cidr}, namespace)
					if createErr == nil {
						createOK = true
					}
				}()
				go func() {
					defer wg.Done()
					<-start
					deleteErr = pair.a.DeleteNamespace(ctx, namespace)
					if deleteErr == nil {
						deleteOK = true
					}
				}()
				close(start)
				wg.Wait()

				// The core invariant: never both successful.
				require.False(t, createOK && deleteOK,
					"round %d: create and delete both succeeded (createErr=%v deleteErr=%v)", round, createErr, deleteErr)

				switch {
				case createOK:
					// Create won: delete must report the conflict and the
					// prefix must be retained on every instance.
					require.Error(t, deleteErr, "round %d", round)
					require.True(t, errors.Is(deleteErr, ErrNamespaceNotEmpty),
						"round %d expected non-empty conflict, got %v", round, deleteErr)
					p, err := pair.a.ReadPrefix(ctx, cidr, namespace)
					require.NoError(t, err, "round %d", round)
					require.Equal(t, cidr, p.Cidr)
					p, err = witness.ReadPrefix(ctx, cidr, namespace)
					require.NoError(t, err, "round %d", round)
					require.Equal(t, cidr, p.Cidr)
					// cleanup: empty the namespace, then delete it
					_, err = pair.a.DeletePrefix(ctx, p, namespace)
					require.NoError(t, err, "round %d", round)
					require.NoError(t, pair.a.DeleteNamespace(ctx, namespace), "round %d")
				case deleteOK:
					// Delete won: create must see a missing namespace and
					// nothing must be listed anywhere.
					require.Error(t, createErr, "round %d", round)
					require.True(t, errors.Is(createErr, ErrNamespaceDoesNotExist),
						"round %d expected namespace missing, got %v", round, createErr)
					assertNamespaceListed(t, ctx, pair.a, namespace, false)
					assertNamespaceListed(t, ctx, pair.b, namespace, false)
					assertNamespaceListed(t, ctx, witness, namespace, false)
				default:
					t.Fatalf("round %d: neither side succeeded: createErr=%v deleteErr=%v", round, createErr, deleteErr)
				}
			}
		})
		pair.dispose()
	}
}

// TestFileNamespace_PersistFailureAndRecovery injects a physical storage
// failure (the state file can not be committed) and verifies that the local
// cache is rolled back to the durable state, that a reopened instance sees
// exactly the same state, and that the next healthy operation works.
func TestFileNamespace_PersistFailureAndRecovery(t *testing.T) {
	ctx := t.Context()

	f, err := newLocalFileWithCleanup(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.cleanup(ctx) })

	injectedErr := errors.New("simulated physical storage failure")

	t.Run("delete failure keeps an empty namespace", func(t *testing.T) {
		const namespace = "file-fail-delete"
		require.NoError(t, f.CreateNamespace(ctx, namespace))

		// Independent instance with a cold cache acting as the disk witness.
		witness, err := newFile(ctx, f.path)
		require.NoError(t, err)
		assertNamespaceListed(t, ctx, witness, namespace, true)

		f.commitFile = func(string, []byte) error { return injectedErr }
		t.Cleanup(func() { f.commitFile = f.writeAndRename })

		err = f.DeleteNamespace(ctx, namespace)
		require.Error(t, err)
		require.ErrorIs(t, err, injectedErr)

		// Same-instance cache was rolled back: reads (which do not persist)
		// still show the namespace.
		assertNamespaceListed(t, ctx, f, namespace, true)
		cidrs, err := f.ReadAllPrefixCidrs(ctx, namespace)
		require.NoError(t, err)
		require.Empty(t, cidrs)

		// The durable state was never modified.
		assertNamespaceListed(t, ctx, witness, namespace, true)

		// A fresh reopen reaches the same conclusion (cache recovery).
		reopened, err := newFile(ctx, f.path)
		require.NoError(t, err)
		assertNamespaceListed(t, ctx, reopened, namespace, true)
		cidrs, err = reopened.ReadAllPrefixCidrs(ctx, namespace)
		require.NoError(t, err)
		require.Empty(t, cidrs)
	})

	t.Run("next healthy operation after recovery", func(t *testing.T) {
		const namespace = "file-fail-recover"
		const cidr = "10.47.0.0/24"
		require.NoError(t, f.CreateNamespace(ctx, namespace))

		// A failed first-prefix creation must roll the cache back as well.
		f.commitFile = func(string, []byte) error { return injectedErr }
		_, err := f.CreatePrefix(ctx, Prefix{Cidr: cidr}, namespace)
		require.ErrorIs(t, err, injectedErr)
		f.commitFile = f.writeAndRename

		cidrs, err := f.ReadAllPrefixCidrs(ctx, namespace)
		require.NoError(t, err)
		require.Empty(t, cidrs, "phantom prefix must not remain in the rolled back cache")

		reopened, err := newFile(ctx, f.path)
		require.NoError(t, err)
		cidrs, err = reopened.ReadAllPrefixCidrs(ctx, namespace)
		require.NoError(t, err)
		require.Empty(t, cidrs)

		// Health restored: create the prefix for real, it is durably visible
		// to other instances and survives another reopen.
		created, err := f.CreatePrefix(ctx, Prefix{Cidr: cidr}, namespace)
		require.NoError(t, err)
		require.Equal(t, cidr, created.Cidr)

		witness, err := newFile(ctx, f.path)
		require.NoError(t, err)
		p, err := witness.ReadPrefix(ctx, cidr, namespace)
		require.NoError(t, err)
		require.Equal(t, cidr, p.Cidr)

		// Non-empty rejection still works on the recovered, healthy backend.
		err = f.DeleteNamespace(ctx, namespace)
		require.ErrorIs(t, err, ErrNamespaceNotEmpty)

		_, err = f.DeletePrefix(ctx, created, namespace)
		require.NoError(t, err)
		require.NoError(t, f.DeleteNamespace(ctx, namespace))

		final, err := newFile(ctx, f.path)
		require.NoError(t, err)
		assertNamespaceListed(t, ctx, final, namespace, false)
	})
}

// TestNamespaceLifecycle_PhysicalDeleteFailure blocks the physical DROP on
// PostgreSQL with a conflicting table lock held by another connection and lets
// the delete time out: the transaction must roll back, the table must survive
// and the next healthy operation must work.
func TestNamespaceLifecycle_PhysicalDeleteFailure(t *testing.T) {
	ctx := t.Context()

	testWithSQLBackends(t, func(t *testing.T, db *sql) {
		var serverVersion string
		require.NoError(t, db.db.Get(&serverVersion, "SELECT version()"))
		if strings.Contains(strings.ToLower(serverVersion), "cockroachdb") {
			t.Skip("LOCK TABLE is PostgreSQL specific; cockroachDB serialization is covered by the race test")
		}
		const namespace = "lc-physical-fail"
		require.NoError(t, db.CreateNamespace(ctx, namespace))
		t.Cleanup(func() {
			_, _ = db.DeletePrefix(ctx, Prefix{Cidr: "10.48.0.0/24"}, namespace)
			_ = db.DeleteNamespace(ctx, namespace)
		})

		blocker, err := db.db.Begin()
		require.NoError(t, err)
		_, err = blocker.Exec("LOCK TABLE " + getTableName(namespace) + " IN ACCESS EXCLUSIVE MODE")
		require.NoError(t, err)

		timedCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err = db.DeleteNamespace(timedCtx, namespace)
		cancel()
		require.Error(t, err, "blocked delete must fail")
		require.False(t, errors.Is(err, ErrNamespaceDoesNotExist), "namespace must not disappear")
		require.False(t, errors.Is(err, ErrNamespaceNotEmpty), "empty namespace must not look non-empty")

		// Release the blocker: the table still exists and the namespace is
		// usable, i.e. persistent state and table cache stayed consistent.
		require.NoError(t, blocker.Rollback())

		created, err := db.CreatePrefix(ctx, Prefix{Cidr: "10.48.0.0/24"}, namespace)
		require.NoError(t, err)
		err = db.DeleteNamespace(ctx, namespace)
		require.ErrorIs(t, err, ErrNamespaceNotEmpty)
		_, err = db.ReadPrefix(ctx, "10.48.0.0/24", namespace)
		require.NoError(t, err)

		_, err = db.DeletePrefix(ctx, created, namespace)
		require.NoError(t, err)
		require.NoError(t, db.DeleteNamespace(ctx, namespace))
	})
}

// TestFileNamespace_FlockSerializesIndependentInstances verifies the
// cross-instance mutual exclusion primitive itself: a second instance blocks
// for the duration of the first instance's critical section.
func TestFileNamespace_FlockSerializesIndependentInstances(t *testing.T) {
	ctx := t.Context()

	f, err := newLocalFileWithCleanup(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.cleanup(ctx) })

	require.NoError(t, f.begin())
	acquired := make(chan struct{})
	go func() {
		_ = f2Begin(ctx, f.path, acquired)
	}()

	select {
	case <-acquired:
		t.Fatal("second instance entered the critical section while the first held the lock")
	case <-time.After(200 * time.Millisecond):
	}
	f.end()

	select {
	case <-acquired:
	case <-time.After(5 * time.Second):
		t.Fatal("second instance never acquired the lock after release")
	}
}

// f2Begin opens an independent file storage on the same path and takes the
// exclusive lock, signaling once acquired.
func f2Begin(ctx context.Context, path string, acquired chan<- struct{}) error {
	f, err := newFile(ctx, path)
	if err != nil {
		return err
	}
	if err := f.begin(); err != nil {
		return err
	}
	close(acquired)
	f.end()
	return nil
}
