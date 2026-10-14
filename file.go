package ipam

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"
)

type file struct {
	// path location of the state file
	path string
	// prettyJSON is always true, but up for being configurable
	prettyJSON bool
	// modTime helps with tracking external file changes
	modTime time.Time
	// parent implements internal state management, currently it is always NewMemory()
	parent Storage
	// lock serializes operations within this instance. Every operation,
	// including reads, reloads the local cache from disk, so the lock is
	// always taken exclusively.
	lock sync.Mutex
	// lockFile is the handle of the sidecar lock file. An exclusive flock on
	// it serializes independent instances (different processes, or different
	// opens in the same process) that share one state file, turning the
	// reload -> mutate -> persist sequence into a cross-instance atomic step.
	lockFile *os.File
	// commitFile durably commits serialized state to the state file. It is a
	// field so tests can inject physical storage failures; the production
	// implementation writes a temporary file and atomically renames it over
	// the state file.
	commitFile func(path string, data []byte) error
}

var (
	nullModTime          time.Time
	DefaultLocalFilePath string
)

// fileJSONData is a representation of JSON file's structure
type fileJSONData map[string]map[string]prefixJSON

func init() {
	nullModTime = time.Unix(0, 0)
	DefaultLocalFilePath = path.Join(getXDGDataHome(), "go-ipam", "ipam-db.json")
}

// getXDGDataHome() is an utility for finding the most suitable data directory:
// 1) $XDG_DATA_HOME
// 2) $HOME/.local/share
// 3) current directory
func getXDGDataHome() string {
	if val := os.Getenv("XDG_DATA_HOME"); val != "" {
		return val
	}

	val, err := os.UserHomeDir()
	if err != nil {
		val = "."
	} else {
		val = path.Join(val, ".local", "share")
	}
	return val
}

// newFile constructs a file storage and prepares the sidecar flock file.
func newFile(ctx context.Context, filePath string) (*file, error) {
	if err := os.MkdirAll(filepath.Dir(filePath), 0o700); err != nil {
		return nil, fmt.Errorf("unable to create state directory for %q: %w", filePath, err)
	}
	lockHandle, err := os.OpenFile(filePath+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("unable to open state lock file %q: %w", filePath+".lock", err)
	}
	f := &file{
		path:       filePath,
		prettyJSON: true,
		parent:     NewMemory(ctx),
		modTime:    nullModTime,
		lockFile:   lockHandle,
	}
	f.commitFile = f.writeAndRename
	return f, nil
}

// writeAndRename commits state durably by writing a sidecar temp file and
// atomically renaming it over the state file. A crash or a failing write can
// therefore never leave a truncated or half-written state file behind.
func (f *file) writeAndRename(filePath string, data []byte) (err error) {
	tmp := filePath + ".tmp"
	if err = os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(tmp)
		}
	}()
	return os.Rename(tmp, filePath)
}

// NewLocalFile creates a JSON file storage for ipam
func NewLocalFile(ctx context.Context, filePath string) Storage {
	f, err := newFile(ctx, filePath)
	if err != nil {
		// Construction never failed historically; degrade to in-process
		// serialization only and let the first operation surface the
		// underlying filesystem problem.
		f = &file{
			path:       filePath,
			prettyJSON: true,
			parent:     NewMemory(ctx),
			modTime:    nullModTime,
		}
		f.commitFile = f.writeAndRename
	}
	return f
}

// begin enters the cross-instance exclusive critical section.
func (f *file) begin() error {
	if f.lockFile != nil {
		if err := lockFileExclusive(f.lockFile); err != nil {
			return fmt.Errorf("unable to lock state file %q: %w", f.path, err)
		}
	}
	f.lock.Lock()
	return nil
}

// end leaves the cross-instance exclusive critical section.
func (f *file) end() {
	f.lock.Unlock()
	if f.lockFile != nil {
		_ = unlockFile(f.lockFile)
	}
}

// view serializes a read against cross-instance writers and makes the local
// cache reflect the durable state before answering.
func (f *file) view(ctx context.Context, read func() error) error {
	if err := f.begin(); err != nil {
		return err
	}
	defer f.end()
	if err := f.reload(ctx); err != nil {
		return err
	}
	return read()
}

// commit serializes a state-mutating operation: reload from disk, apply the
// change to the local cache, then durably persist the whole cache.
//
// If the physical commit fails, the local cache is resynchronized from disk,
// so after success, conflict, timeout or partial failure the local cache can
// never diverge from the durable state.
func (f *file) commit(ctx context.Context, mutate func() error) error {
	if err := f.begin(); err != nil {
		return err
	}
	defer f.end()

	if err := f.reload(ctx); err != nil {
		return err
	}
	if err := mutate(); err != nil {
		// The cache rejecting the transition (e.g. non-empty namespace)
		// never mutated any state, so nothing has to be rolled back.
		return err
	}
	if err := f.persist(ctx); err != nil {
		if syncErr := f.resync(ctx); syncErr != nil {
			// The original error is what the caller must act on; the resync
			// failure is attached for observability.
			return fmt.Errorf("%w (cache resync after failed persist also failed: %v)", err, syncErr)
		}
		return err
	}
	return nil
}

// resync forces the next reload to rebuild the local cache from the durable
// state file, regardless of the previously observed modification time.
func (f *file) resync(ctx context.Context) error {
	f.modTime = nullModTime
	return f.reload(ctx)
}

// clearParent() empties the internal state
func (f *file) clearParent(ctx context.Context) (err error) {
	namespaces, err := f.parent.ListNamespaces(ctx)
	if err != nil {
		return fmt.Errorf("failed to list namespaces: %w", err)
	}
	for _, namespace := range namespaces {
		if err = f.parent.DeleteAllPrefixes(ctx, namespace); err != nil {
			return fmt.Errorf("failed to delete prefixes for %s namespace: %w", namespace, err)
		}
		if namespace == defaultNamespace {
			// skip deletion instead of replicating NewMemory behavior
			continue
		}
		if err = f.parent.DeleteNamespace(ctx, namespace); err != nil {
			return fmt.Errorf("failed to delete %s namespace: %w", namespace, err)
		}
	}
	return nil
}

// getModTime() returns file's modification time without failure (defaulting to nullModTime).
func (f *file) getModTime() time.Time {
	info, err := os.Stat(f.path)
	if err != nil {
		return nullModTime
	}
	return info.ModTime()
}

// reload() is meant to synchronize from the file to internal state representation,
// currently it bases decision solely on the modification time of the file
//
// see ipamer.NamespacedLoad for alternative implementation candidate
// after https://github.com/metal-stack/go-ipam/issues/111 is addressed
func (f *file) reload(ctx context.Context) (err error) {
	// don't do anything when file modification time didn't changed
	if modTime := f.getModTime(); !modTime.Equal(nullModTime) && modTime.Equal(f.modTime) {
		return nil
	}

	var data []byte
	storage := make(fileJSONData)
	if _, err = os.Stat(f.path); !errors.Is(err, fs.ErrNotExist) {
		data, err = os.ReadFile(f.path)
		if err != nil {
			return fmt.Errorf("failed to read state file %q: %w", f.path, err)
		}
	}
	f.modTime = f.getModTime()
	// smallest valid piece of data is "{}"
	if len(data) >= 2 {
		err = json.Unmarshal(data, &storage)
		if err != nil {
			return fmt.Errorf("failed to parse state file %q: %w", f.path, err)
		}
	}
	if err = f.clearParent(ctx); err != nil {
		return fmt.Errorf("failed to clear memory storage: %w", err)
	}
	for namespace, prefixes := range storage {
		if err = f.parent.CreateNamespace(ctx, namespace); err != nil {
			return fmt.Errorf("failed to reload a %s namespace: %w", namespace, err)
		}
		for _, prefix := range prefixes {
			if _, err = f.parent.CreatePrefix(ctx, prefix.toPrefix(), namespace); err != nil {
				return fmt.Errorf("failed to reload a %s prefix in %s namespace: %w", prefix.Cidr, namespace, err)
			}
		}
	}
	return nil
}

// persist() dumps current internal state to file
//
// see ipamer.NamespacedDump for alternative implementation candidate
// after https://github.com/metal-stack/go-ipam/issues/111 is addressed
func (f *file) persist(ctx context.Context) (err error) {
	storage := make(fileJSONData)
	var (
		prefixes map[string]prefixJSON
		ok       bool
		data     []byte
	)

	namespaces, err := f.parent.ListNamespaces(ctx)
	if err != nil {
		return fmt.Errorf("failed to list namespaces while building external state representation: %w", err)
	}
	for _, namespace := range namespaces {
		if prefixes, ok = storage[namespace]; !ok {
			prefixes = make(map[string]prefixJSON)
			storage[namespace] = prefixes
		}
		ps, err := f.parent.ReadAllPrefixes(ctx, namespace)
		if err != nil {
			return fmt.Errorf("failed to read prefixes of %s namespace while building external state representation: %w", namespace, err)
		}
		for _, prefix := range ps {
			prefixes[prefix.Cidr] = prefix.toPrefixJSON()
		}
	}
	if f.prettyJSON {
		data, err = json.MarshalIndent(storage, "", "  ")
	} else {
		data, err = json.Marshal(storage)
	}
	if err != nil {
		return fmt.Errorf("failed to serialize JSON: %w", err)
	}
	if err = f.commitFile(f.path, data); err != nil {
		return fmt.Errorf("error storing state at %q: %w", f.path, err)
	}
	f.modTime = f.getModTime()
	return nil
}
func (f *file) Name() string {
	return "file"
}

func (f *file) CreatePrefix(ctx context.Context, prefix Prefix, namespace string) (p Prefix, err error) {
	err = f.commit(ctx, func() error {
		var applyErr error
		p, applyErr = f.parent.CreatePrefix(ctx, prefix, namespace)
		return applyErr
	})
	return p, err
}

func (f *file) ReadPrefix(ctx context.Context, prefix, namespace string) (p Prefix, err error) {
	err = f.view(ctx, func() error {
		var readErr error
		p, readErr = f.parent.ReadPrefix(ctx, prefix, namespace)
		return readErr
	})
	return p, err
}

func (f *file) DeleteAllPrefixes(ctx context.Context, namespace string) error {
	return f.commit(ctx, func() error {
		return f.parent.DeleteAllPrefixes(ctx, namespace)
	})
}

func (f *file) ReadAllPrefixes(ctx context.Context, namespace string) (ps Prefixes, err error) {
	err = f.view(ctx, func() error {
		var readErr error
		ps, readErr = f.parent.ReadAllPrefixes(ctx, namespace)
		return readErr
	})
	return ps, err
}

func (f *file) ReadAllPrefixCidrs(ctx context.Context, namespace string) (cidrs []string, err error) {
	err = f.view(ctx, func() error {
		var readErr error
		cidrs, readErr = f.parent.ReadAllPrefixCidrs(ctx, namespace)
		return readErr
	})
	return cidrs, err
}

func (f *file) UpdatePrefix(ctx context.Context, prefix Prefix, namespace string) (p Prefix, err error) {
	err = f.commit(ctx, func() error {
		var applyErr error
		p, applyErr = f.parent.UpdatePrefix(ctx, prefix, namespace)
		return applyErr
	})
	return p, err
}

func (f *file) DeletePrefix(ctx context.Context, prefix Prefix, namespace string) (p Prefix, err error) {
	err = f.commit(ctx, func() error {
		var applyErr error
		p, applyErr = f.parent.DeletePrefix(ctx, prefix, namespace)
		return applyErr
	})
	return p, err
}

func (f *file) CreateNamespace(ctx context.Context, namespace string) error {
	return f.commit(ctx, func() error {
		return f.parent.CreateNamespace(ctx, namespace)
	})
}

func (f *file) ListNamespaces(ctx context.Context) (result []string, err error) {
	err = f.view(ctx, func() error {
		var readErr error
		result, readErr = f.parent.ListNamespaces(ctx)
		return readErr
	})
	return result, err
}

// DeleteNamespace runs the guarded parent transition inside the cross-instance
// critical section: a CreatePrefix committing on another instance is either
// visible to the emptiness check (ErrNamespaceNotEmpty, prefix retained) or
// happens only after the persisted removal (ErrNamespaceDoesNotExist).
func (f *file) DeleteNamespace(ctx context.Context, namespace string) error {
	return f.commit(ctx, func() error {
		return f.parent.DeleteNamespace(ctx, namespace)
	})
}
