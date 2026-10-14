//go:build windows

package ipam

import "os"

// Windows builds keep the in-process serialization only. Cross-process
// coordination is not provided on this platform.
func lockFileExclusive(_ *os.File) error { return nil }

func unlockFile(_ *os.File) error { return nil }
