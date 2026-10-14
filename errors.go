package ipam

import (
	"errors"
	"fmt"
)

var (
	// ErrNotFound is returned if prefix or cidr was not found
	ErrNotFound = errors.New("NotFound")
	// ErrNoIPAvailable is returned if no IP is available anymore
	ErrNoIPAvailable = errors.New("NoIPAvailableError")
	// ErrAlreadyAllocated is returned if the requested address is not available
	ErrAlreadyAllocated = errors.New("AlreadyAllocatedError")
	// ErrOptimisticLockError is returned if insert or update conflicts with the existing data
	ErrOptimisticLockError = errors.New("OptimisticLockError")
	// ErrNamespaceDoesNotExist is returned when an operation is performed in a namespace that does not exist.
	ErrNamespaceDoesNotExist = errors.New("NamespaceDoesNotExist")
	// ErrNamespaceNotEmpty is returned when a namespace can not be deleted because
	// it still contains prefixes. It is the conflict outcome of the atomic
	// DeleteNamespace lifecycle transition: a concurrent CreatePrefix that
	// committed first wins and its prefix is retained.
	ErrNamespaceNotEmpty = errors.New("NamespaceNotEmpty")
	// ErrNameTooLong is returned when a name exceeds the databases max identifier length
	ErrNameTooLong = errors.New("NameTooLong")
)

// errNamespaceHasPrefixes keeps the historical non-empty rejection message
// stable while allowing callers to detect the conflict via errors.Is.
var errNamespaceHasPrefixes = fmt.Errorf("cannot delete namespace with allocated prefixes: %w", ErrNamespaceNotEmpty)
