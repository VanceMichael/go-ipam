//go:build !windows

package ipam

import (
	"os"

	"golang.org/x/sys/unix"
)

// lockFileExclusive blocks until an exclusive advisory lock on the given file
// descriptor is held. The lock is bound to the open file description, so it
// also serializes independent file storage instances (separate opens) inside
// the same process, which is exactly the guarantee required for the
// DeleteNamespace/CreatePrefix lifecycle transition.
func lockFileExclusive(f *os.File) error {
	return unix.Flock(int(f.Fd()), unix.LOCK_EX)
}

func unlockFile(f *os.File) error {
	return unix.Flock(int(f.Fd()), unix.LOCK_UN)
}
