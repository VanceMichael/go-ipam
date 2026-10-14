package ipam

import (
	"context"
	"errors"
	"fmt"
	"sync"

	redigo "github.com/redis/go-redis/v9"
)

const namespaceKey = "namespaces"

// Return codes shared by the lifecycle Lua scripts.
const (
	redisLuaOK               = 0
	redisLuaNamespaceMissing = -1
	redisLuaPrefixExists     = -2
)

// createPrefixScript atomically gates prefix creation on namespace
// membership, so a committed DeleteNamespace makes a concurrent create fail
// with redisLuaNamespaceMissing instead of resurrecting the prefix hash.
//
// KEYS[1]: namespaces set, KEYS[2]: prefix hash of the namespace
// ARGV[1]: namespace, ARGV[2]: cidr, ARGV[3]: serialized prefix
var createPrefixScript = redigo.NewScript(`
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
	return -1
end
if redis.call('HEXISTS', KEYS[2], ARGV[2]) == 1 then
	return -2
end
redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
return 0
`)

// deleteNamespaceScript is the single-step atomic lifecycle transition:
// membership check + emptiness check + directory/resource removal execute
// without any other command being able to interleave (Redis runs scripts
// atomically).
//
// KEYS[1]: namespaces set, KEYS[2]: prefix hash of the namespace
// ARGV[1]: namespace
var deleteNamespaceScript = redigo.NewScript(`
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
	return -1
end
if redis.call('HLEN', KEYS[2]) ~= 0 then
	return -2
end
redis.call('SREM', KEYS[1], ARGV[1])
redis.call('DEL', KEYS[2])
return 0
`)

type redis struct {
	rdb        *redigo.Client
	namespaces map[string]struct{}
	lock       sync.RWMutex
}

// NewRedis create a redis storage for ipam
func NewRedis(ctx context.Context, ip, port string) (Storage, error) {
	return newRedis(ctx, ip, port)
}
func (r *redis) Name() string {
	return "redis"
}

func newRedis(ctx context.Context, ip, port string) (*redis, error) {
	rdb := redigo.NewClient(&redigo.Options{
		Addr:     fmt.Sprintf("%s:%s", ip, port),
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	r := &redis{
		rdb:        rdb,
		namespaces: make(map[string]struct{}),
		lock:       sync.RWMutex{},
	}
	if err := r.CreateNamespace(ctx, defaultNamespace); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *redis) checkNamespaceExists(ctx context.Context, namespace string) error {
	if _, ok := r.namespaces[namespace]; ok {
		return nil
	}
	found, err := r.rdb.SIsMember(ctx, namespaceKey, namespace).Result()
	if err != nil {
		return fmt.Errorf("error checking namespace: %w", err)
	}
	if !found {
		return ErrNamespaceDoesNotExist
	}
	r.namespaces[namespace] = struct{}{}
	return nil
}

func (r *redis) CreatePrefix(ctx context.Context, prefix Prefix, namespace string) (Prefix, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	pfx, err := prefix.toJSON()
	if err != nil {
		return Prefix{}, err
	}

	// The membership decision happens server-side inside the script, so it
	// is linearizable with deleteNamespaceScript even across independent
	// clients sharing this Redis.
	result, err := createPrefixScript.Run(ctx, r.rdb,
		[]string{namespaceKey, namespace},
		namespace, prefix.Cidr, pfx,
	).Int64()
	if err != nil {
		return Prefix{}, fmt.Errorf("unable to create prefix:%v, error:%w", prefix, err)
	}
	switch result {
	case redisLuaOK:
		return prefix, nil
	case redisLuaNamespaceMissing:
		// A concurrent delete committed: make sure a stale positive cache
		// entry cannot hide that fact from this client.
		delete(r.namespaces, namespace)
		return Prefix{}, ErrNamespaceDoesNotExist
	case redisLuaPrefixExists:
		return Prefix{}, fmt.Errorf("prefix:%v already exists", prefix)
	default:
		return Prefix{}, fmt.Errorf("unexpected create prefix result:%d", result)
	}
}
func (r *redis) ReadPrefix(ctx context.Context, prefix, namespace string) (Prefix, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	if err := r.checkNamespaceExists(ctx, namespace); err != nil {
		return Prefix{}, err
	}

	result, err := r.rdb.HGet(ctx, namespace, prefix).Result()
	if err != nil {
		return Prefix{}, fmt.Errorf("%w unable to read existing prefix:%v, error:%w", ErrNotFound, prefix, err)
	}
	return fromJSON([]byte(result))
}

func (r *redis) DeleteAllPrefixes(ctx context.Context, namespace string) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if err := r.checkNamespaceExists(ctx, namespace); err != nil {
		return err
	}
	return r.rdb.Del(ctx, namespace).Err()
}

func (r *redis) ReadAllPrefixes(ctx context.Context, namespace string) (Prefixes, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	if err := r.checkNamespaceExists(ctx, namespace); err != nil {
		return nil, err
	}

	pfxs, err := r.rdb.HGetAll(ctx, namespace).Result()
	if err != nil {
		return nil, fmt.Errorf("unable to get all prefix cidrs:%w", err)
	}
	result := Prefixes{}
	for _, pfx := range pfxs {
		pfx, err := fromJSON([]byte(pfx))
		if err != nil {
			return nil, err
		}
		result = append(result, pfx)
	}
	return result, nil
}
func (r *redis) ReadAllPrefixCidrs(ctx context.Context, namespace string) ([]string, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	if err := r.checkNamespaceExists(ctx, namespace); err != nil {
		return nil, err
	}

	pfxs, err := r.rdb.HGetAll(ctx, namespace).Result()
	if err != nil {
		return nil, fmt.Errorf("unable to get all prefix cidrs:%w", err)
	}
	ps := make([]string, 0, len(pfxs))
	for cidr := range pfxs {
		ps = append(ps, cidr)
	}
	return ps, nil
}
func (r *redis) UpdatePrefix(ctx context.Context, prefix Prefix, namespace string) (Prefix, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if err := r.checkNamespaceExists(ctx, namespace); err != nil {
		return Prefix{}, err
	}

	oldVersion := prefix.version
	prefix.version = oldVersion + 1
	pn, err := prefix.toJSON()
	if err != nil {
		return Prefix{}, err
	}

	txf := func(tx *redigo.Tx) error {
		// Re-check membership inside the watched transaction: a concurrent
		// DeleteNamespace either removes the set membership or deletes the
		// prefix hash, both of which abort this transaction.
		member, err := tx.SIsMember(ctx, namespaceKey, namespace).Result()
		if err != nil {
			return err
		}
		if !member {
			return ErrNamespaceDoesNotExist
		}
		// Get current value or zero.
		p, err := tx.HGet(ctx, namespace, prefix.Cidr).Result()
		if err != nil && !errors.Is(err, redigo.Nil) {
			return err
		}
		oldPrefix, err := fromJSON([]byte(p))
		if err != nil {
			return err
		}
		// Actual operation (local in optimistic lock).
		if oldPrefix.version != oldVersion {
			return fmt.Errorf("%w: unable to update prefix:%s", ErrOptimisticLockError, prefix.Cidr)
		}

		// Operation is committed only if the watched keys remain unchanged.
		_, err = tx.TxPipelined(ctx, func(pipe redigo.Pipeliner) error {
			pipe.HSet(ctx, namespace, prefix.Cidr, pn)
			return nil
		})
		return err
	}
	// Watch both the prefix hash and the namespace directory: the delete
	// transition modifies both (DEL hash, SREM set).
	err = r.rdb.Watch(ctx, txf, namespace, namespaceKey)
	if err != nil {
		if errors.Is(err, redigo.TxFailedErr) {
			// Concurrent namespace deletion or another committer changed a
			// watched key; surface as optimistic lock conflict so callers can
			// re-read and retry.
			return Prefix{}, fmt.Errorf("%w: unable to update prefix:%s", ErrOptimisticLockError, prefix.Cidr)
		}
		if errors.Is(err, ErrNamespaceDoesNotExist) {
			delete(r.namespaces, namespace)
		}
		return Prefix{}, err
	}

	return prefix, nil
}
func (r *redis) DeletePrefix(ctx context.Context, prefix Prefix, namespace string) (Prefix, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if err := r.checkNamespaceExists(ctx, namespace); err != nil {
		return Prefix{}, err
	}

	if err := r.rdb.HDel(ctx, namespace, prefix.Cidr).Err(); err != nil {
		return *prefix.deepCopy(), err
	}
	return *prefix.deepCopy(), nil
}

func (r *redis) CreateNamespace(ctx context.Context, namespace string) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if _, ok := r.namespaces[namespace]; ok {
		return nil
	}
	if err := r.rdb.SAdd(ctx, namespaceKey, namespace).Err(); err != nil {
		return err
	}
	r.namespaces[namespace] = struct{}{}

	return nil
}

func (r *redis) ListNamespaces(ctx context.Context) ([]string, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.rdb.SMembers(ctx, namespaceKey).Result()
}

// DeleteNamespace executes the emptiness check and the directory/resource
// removal in a single server-side Lua transaction. Independent clients can
// never observe the two steps as separated, so create and delete can not both
// succeed.
func (r *redis) DeleteNamespace(ctx context.Context, namespace string) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	result, err := deleteNamespaceScript.Run(ctx, r.rdb,
		[]string{namespaceKey, namespace},
		namespace,
	).Int64()
	if err != nil {
		return fmt.Errorf("unable to delete namespace:%s, error:%w", namespace, err)
	}
	switch result {
	case redisLuaOK:
		// Commit observed: drop the positive cache entry.
		delete(r.namespaces, namespace)
		return nil
	case redisLuaNamespaceMissing:
		delete(r.namespaces, namespace)
		return ErrNamespaceDoesNotExist
	case redisLuaPrefixExists:
		// Conflict outcome: a create committed first, everything is retained.
		return errNamespaceHasPrefixes
	default:
		return fmt.Errorf("unexpected delete namespace result:%d", result)
	}
}
