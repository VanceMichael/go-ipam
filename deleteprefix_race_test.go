package ipam

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// interleaverStorage wraps a Storage and blocks the delete/update decisions at
// controllable points. Two independent Ipamer instances over the same underlying
// storage can therefore be interleaved deterministically, as if they were two
// service instances racing on the same prefix.
type interleaverStorage struct {
	Storage
	beforeDeletePrefix func()
	beforeUpdatePrefix func()
}

func (s *interleaverStorage) DeletePrefix(ctx context.Context, prefix Prefix, namespace string) (Prefix, error) {
	if s.beforeDeletePrefix != nil {
		s.beforeDeletePrefix()
	}
	return s.Storage.DeletePrefix(ctx, prefix, namespace)
}

func (s *interleaverStorage) UpdatePrefix(ctx context.Context, prefix Prefix, namespace string) (Prefix, error) {
	if s.beforeUpdatePrefix != nil {
		s.beforeUpdatePrefix()
	}
	return s.Storage.UpdatePrefix(ctx, prefix, namespace)
}

// gate blocks the wrapped call at the hook until the test releases it and
// signals that the racing operation has reached the hook.
type gate struct {
	onceEntered sync.Once
	onceRelease sync.Once
	entered     chan struct{}
	proceed     chan struct{}
}

func newGate() *gate {
	return &gate{
		entered: make(chan struct{}),
		proceed: make(chan struct{}),
	}
}

func (g *gate) hook() {
	g.onceEntered.Do(func() { close(g.entered) })
	<-g.proceed
}

func (g *gate) waitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the racing operation to reach its interleave point")
	}
}

func (g *gate) release() {
	g.onceRelease.Do(func() { close(g.proceed) })
}

const (
	raceCIDRAcquireWins = "192.168.77.0/30"
	raceCIDRDeleteWins  = "192.168.88.0/30"
)

// raceHolds are the two independent ipamer instances plus a setup instance.
type raceHolds struct {
	setup Ipamer
	del   Ipamer
	acq   Ipamer
	// settle is invoked between the winning commit and releasing the loser. File
	// instances synchronize through file modification times, so a small delay
	// keeps the interleaving deterministic on coarse filesystems.
	settle func()
}

// runAcquireCommitsFirst proves that when AcquireIP commits before the
// DeletePrefix decision, the delete is rejected with a recognizable conflict
// and the allocated address survives. The recovery path (release the ip,
// retry the delete) is exercised as well.
func runAcquireCommitsFirst(t *testing.T, h raceHolds) {
	ctx := t.Context()

	_, err := h.setup.NewPrefix(ctx, raceCIDRAcquireWins)
	require.NoError(t, err)

	deleteGate := newGate()
	switch d := h.del.(*ipamer).storage.(type) {
	case *interleaverStorage:
		d.beforeDeletePrefix = deleteGate.hook
	}

	type deleteResult struct {
		prefix *Prefix
		err    error
	}
	deleteDone := make(chan deleteResult, 1)
	go func() {
		p, err := h.del.DeletePrefix(ctx, raceCIDRAcquireWins)
		deleteDone <- deleteResult{p, err}
	}()

	// The deleter has observed the empty prefix and is now parked right before
	// the atomic delete decision.
	deleteGate.waitEntered(t)

	// Another instance acquires an address and fully commits it.
	ip, err := h.acq.AcquireIP(ctx, raceCIDRAcquireWins)
	require.NoError(t, err, "the acquiring instance must commit successfully")
	require.NotNil(t, ip)
	require.Equal(t, "192.168.77.1", ip.IP.String())

	h.settle()
	deleteGate.release()

	res := <-deleteDone
	require.Nil(t, res.prefix)
	require.Error(t, res.err)
	require.ErrorIs(t, res.err, ErrOptimisticLockError, "delete must report a recognizable conflict, got: %v", res.err)

	// The prefix and the just-returned address must survive on both instances.
	for name, instance := range map[string]Ipamer{"deleter": h.del, "acquirer": h.acq} {
		p, err := instance.PrefixFrom(ctx, raceCIDRAcquireWins)
		require.NoError(t, err, "prefix must still exist on the %s instance", name)
		require.NotNil(t, p)
		require.Contains(t, p.ips, ip.IP.String(), "allocated ip must survive on the %s instance", name)
	}

	// Recovery path: release the address, then the delete can succeed.
	require.NoError(t, h.acq.ReleaseIPFromPrefix(ctx, raceCIDRAcquireWins, ip.IP.String()))
	_, err = h.del.DeletePrefix(ctx, raceCIDRAcquireWins)
	require.NoError(t, err)

	// A second delete is a stable not-found.
	_, err = h.del.DeletePrefix(ctx, raceCIDRAcquireWins)
	require.ErrorIs(t, err, ErrNotFound)
}

// runDeleteCommitsFirst proves that when DeletePrefix commits before the
// AcquireIP decision, the acquire is rejected with a stable not-found instead of
// returning an address of a deleted prefix. The recovery path is an explicit
// prefix recreation followed by a successful acquire.
func runDeleteCommitsFirst(t *testing.T, h raceHolds) {
	ctx := t.Context()

	_, err := h.setup.NewPrefix(ctx, raceCIDRDeleteWins)
	require.NoError(t, err)

	updateGate := newGate()
	switch a := h.acq.(*ipamer).storage.(type) {
	case *interleaverStorage:
		a.beforeUpdatePrefix = updateGate.hook
	}

	type acquireResult struct {
		ip  *IP
		err error
	}
	acquireDone := make(chan acquireResult, 1)
	go func() {
		ip, err := h.acq.AcquireIP(ctx, raceCIDRDeleteWins)
		acquireDone <- acquireResult{ip, err}
	}()

	// The acquirer has observed the existing prefix and is parked right before
	// persisting its allocation.
	updateGate.waitEntered(t)

	// The deleting instance commits first.
	deleted, err := h.del.DeletePrefix(ctx, raceCIDRDeleteWins)
	require.NoError(t, err)
	require.NotNil(t, deleted)

	h.settle()
	updateGate.release()

	res := <-acquireDone
	require.Nil(t, res.ip)
	require.Error(t, res.err)
	require.ErrorIs(t, res.err, ErrNotFound, "acquire after a committed delete must report not-found, got: %v", res.err)
	require.NotErrorIs(t, res.err, ErrOptimisticLockError)

	// The prefix is gone from both instances.
	_, err = h.del.PrefixFrom(ctx, raceCIDRDeleteWins)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = h.acq.PrefixFrom(ctx, raceCIDRDeleteWins)
	require.ErrorIs(t, err, ErrNotFound)

	// Recovery path: explicitly recreate the prefix, then the retry succeeds.
	_, err = h.del.NewPrefix(ctx, raceCIDRDeleteWins)
	require.NoError(t, err)
	h.settle()
	ip, err := h.acq.AcquireIP(ctx, raceCIDRDeleteWins)
	require.NoError(t, err)
	require.NotNil(t, ip)
	require.Equal(t, "192.168.88.1", ip.IP.String())

	// The allocation is visible on the other instance as well.
	p, err := h.del.PrefixFrom(ctx, raceCIDRDeleteWins)
	require.NoError(t, err)
	require.Contains(t, p.ips, ip.IP.String())
}

func TestDeletePrefixAcquireRace_Memory(t *testing.T) {
	ctx := t.Context()

	shared := NewMemory(ctx)
	setup := NewWithStorage(shared)

	t.Run("acquire commits first", func(t *testing.T) {
		h := raceHolds{
			setup:  setup,
			del:    NewWithStorage(&interleaverStorage{Storage: shared}),
			acq:    NewWithStorage(&interleaverStorage{Storage: shared}),
			settle: func() {},
		}
		runAcquireCommitsFirst(t, h)
	})

	t.Run("delete commits first", func(t *testing.T) {
		h := raceHolds{
			setup:  setup,
			del:    NewWithStorage(&interleaverStorage{Storage: shared}),
			acq:    NewWithStorage(&interleaverStorage{Storage: shared}),
			settle: func() {},
		}
		runDeleteCommitsFirst(t, h)
	})
}

func TestDeletePrefixAcquireRace_File(t *testing.T) {
	ctx := t.Context()

	path := filepath.Join(t.TempDir(), "ipam-db.json")
	// Two genuinely independent file-backed instances, like two processes.
	newFileInstance := func() Storage {
		return &interleaverStorage{Storage: NewLocalFile(ctx, path)}
	}

	holds := func() raceHolds {
		return raceHolds{
			setup: NewWithStorage(NewLocalFile(ctx, path)),
			del:   NewWithStorage(newFileInstance()),
			acq:   NewWithStorage(newFileInstance()),
			// file instances reconcile via file modification times
			settle: func() { time.Sleep(25 * time.Millisecond) },
		}
	}

	t.Run("acquire commits first", func(t *testing.T) {
		runAcquireCommitsFirst(t, holds())

		// Reopening the persistence layer shows the same outcome: the prefix was
		// released and deleted, nothing half-deleted remains.
		reopened := NewWithStorage(NewLocalFile(ctx, path))
		cidrs, err := reopened.ReadAllPrefixCidrs(ctx)
		require.NoError(t, err)
		require.NotContains(t, cidrs, raceCIDRAcquireWins)
	})

	t.Run("delete commits first", func(t *testing.T) {
		h := holds()
		runDeleteCommitsFirst(t, h)

		// Reopening the persistence layer shows the recreated prefix with the
		// retried allocation, identical to what the live instances observe.
		reopened := NewWithStorage(NewLocalFile(ctx, path))
		p, err := reopened.PrefixFrom(ctx, raceCIDRDeleteWins)
		require.NoError(t, err)
		require.NotNil(t, p)
		require.Contains(t, p.ips, "192.168.88.1")

		cidrs, err := reopened.ReadAllPrefixCidrs(ctx)
		require.NoError(t, err)
		require.Contains(t, cidrs, raceCIDRDeleteWins)
		require.NotContains(t, cidrs, raceCIDRAcquireWins)
	})
}

// TestStorage_DeletePrefixCAS exercises the storage-level compare-and-delete
// contract on every built-in backend: a stale delete conflicts, a current
// delete succeeds and a delete of a missing row is not-found.
func TestStorage_DeletePrefixCAS(t *testing.T) {
	ctx := t.Context()

	testWithBackends(t, func(t *testing.T, ipam *ipamer) {
		storage := ipam.storage

		cidr := "172.31.9.0/30"
		_, err := ipam.NewPrefix(ctx, cidr)
		require.NoError(t, err)

		stale, err := storage.ReadPrefix(ctx, cidr, defaultNamespace)
		require.NoError(t, err)

		// A concurrent allocation bumps the version.
		acquired, err := ipam.AcquireIP(ctx, cidr)
		require.NoError(t, err)

		// The stale delete must lose the atomic arbitration.
		_, err = storage.DeletePrefix(ctx, stale, defaultNamespace)
		require.ErrorIs(t, err, ErrOptimisticLockError)

		// Prefix and address are still there.
		p, err := storage.ReadPrefix(ctx, cidr, defaultNamespace)
		require.NoError(t, err)
		require.Contains(t, p.ips, acquired.IP.String())

		// After the address is released the prefix is empty and a fresh delete
		// succeeds.
		require.NoError(t, ipam.ReleaseIPFromPrefix(ctx, cidr, acquired.IP.String()))
		fresh, err := storage.ReadPrefix(ctx, cidr, defaultNamespace)
		require.NoError(t, err)
		deleted, err := storage.DeletePrefix(ctx, fresh, defaultNamespace)
		require.NoError(t, err)
		require.Equal(t, cidr, deleted.Cidr)

		// Deleting again is a stable not-found, even with the stale snapshot.
		_, err = storage.DeletePrefix(ctx, fresh, defaultNamespace)
		require.ErrorIs(t, err, ErrNotFound)
		_, err = storage.ReadPrefix(ctx, cidr, defaultNamespace)
		require.ErrorIs(t, err, ErrNotFound)
	})
}

// TestFile_PersistFailureRollback verifies that a failed state-file save does
// not make the in-memory view disappear or advance ahead of the durable state.
func TestFile_PersistFailureRollback(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("read-only file permissions are bypassed when running as root")
	}

	ctx := t.Context()
	dir := t.TempDir()
	path := filepath.Join(dir, "ipam-db.json")
	storage := NewLocalFile(ctx, path)
	ipam := NewWithStorage(storage)

	cidr := "192.168.55.0/30"
	_, err := ipam.NewPrefix(ctx, cidr)
	require.NoError(t, err)

	// The atomic save creates a temporary file next to the state file: making
	// the directory read-only forces every persist to fail while reads still
	// work. Restore it before the test directory is cleaned up.
	require.NoError(t, os.Chmod(dir, 0500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0700) })

	// A failed acquire must not leak the address into the in-memory view.
	_, err = ipam.AcquireIP(ctx, cidr)
	require.Error(t, err)
	p, err := ipam.PrefixFrom(ctx, cidr)
	require.NoError(t, err)
	require.NotContains(t, p.ips, "192.168.55.1")

	// A failed delete must not remove the prefix from the in-memory view.
	_, err = ipam.DeletePrefix(ctx, cidr)
	require.Error(t, err)
	p, err = ipam.PrefixFrom(ctx, cidr)
	require.NoError(t, err, "the prefix must remain visible after a failed save")
	require.NotNil(t, p)

	// The state file stayed intact and parseable.
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var onDisk map[string]map[string]prefixJSON
	require.NoError(t, json.Unmarshal(raw, &onDisk))
	require.Contains(t, onDisk, defaultNamespace)
	require.Contains(t, onDisk[defaultNamespace], cidr)
	require.NotContains(t, onDisk[defaultNamespace][cidr].IPs, "192.168.55.1")

	// Once writes work again both operations succeed and a reopened instance
	// observes the durable outcome.
	require.NoError(t, os.Chmod(dir, 0700))
	ip, err := ipam.AcquireIP(ctx, cidr)
	require.NoError(t, err)
	require.Equal(t, "192.168.55.1", ip.IP.String())
	require.NoError(t, ipam.ReleaseIPFromPrefix(ctx, cidr, ip.IP.String()))
	_, err = ipam.DeletePrefix(ctx, cidr)
	require.NoError(t, err)

	reopened := NewWithStorage(NewLocalFile(ctx, path))
	_, err = reopened.PrefixFrom(ctx, cidr)
	require.ErrorIs(t, err, ErrNotFound)
}
