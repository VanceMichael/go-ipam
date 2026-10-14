package ipam

import "context"

// Storage is a interface to store ipam objects.
type Storage interface {
	Name() string
	CreatePrefix(ctx context.Context, prefix Prefix, namespace string) (Prefix, error)
	ReadPrefix(ctx context.Context, prefix string, namespace string) (Prefix, error)
	DeleteAllPrefixes(ctx context.Context, namespace string) error
	ReadAllPrefixes(ctx context.Context, namespace string) (Prefixes, error)
	ReadAllPrefixCidrs(ctx context.Context, namespace string) ([]string, error)
	UpdatePrefix(ctx context.Context, prefix Prefix, namespace string) (Prefix, error)
	DeletePrefix(ctx context.Context, prefix Prefix, namespace string) (Prefix, error)
	CreateNamespace(ctx context.Context, namespace string) error
	ListNamespaces(ctx context.Context) ([]string, error)
	// DeleteNamespace is an atomic lifecycle transition: it decides whether the
	// namespace is empty and removes the namespace directory together with its
	// (empty) prefix resources in a single, cross-instance linearizable step.
	// It returns ErrNamespaceDoesNotExist when the namespace is already gone
	// (CreatePrefix must then fail with the same error until CreateNamespace
	// rebuilds it explicitly) and ErrNamespaceNotEmpty when a prefix committed
	// first, in which case all existing prefixes are retained. The emptiness
	// check and the physical deletion must never be observable as separate
	// steps, so implementations must not force-delete a non-empty namespace.
	DeleteNamespace(ctx context.Context, namespace string) error
}
