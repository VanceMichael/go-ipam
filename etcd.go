package ipam

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// etcdCASRetries bounds compare-and-swap retries on hot namespace markers.
const etcdCASRetries = 100

type etcd struct {
	etcdDB     *clientv3.Client
	namespaces map[string]struct{}
	lock       sync.RWMutex
}

// NewEtcd create a etcd storage for ipam
func NewEtcd(ctx context.Context, ip, port string, cert, key []byte, insecureskip bool) (Storage, error) {
	return newEtcd(ctx, ip, port, cert, key, insecureskip)
}

func (e *etcd) Name() string {
	return "etcd"
}

func newEtcd(ctx context.Context, ip, port string, cert, key []byte, insecureskip bool) (*etcd, error) {
	etcdConfig := clientv3.Config{
		Endpoints:   []string{fmt.Sprintf("%s:%s", ip, port)},
		DialTimeout: 5 * time.Second,
		Context:     context.Background(),
	}

	if cert != nil && key != nil {
		// SSL
		clientCert, err := tls.X509KeyPair(cert, key)
		if err != nil {
			log.Fatal(err)
		}
		tls := &tls.Config{
			Certificates: []tls.Certificate{clientCert},
			// nolint:gosec
			// #nosec G402
			InsecureSkipVerify: insecureskip,
		}
		etcdConfig.TLS = tls
	}
	cli, err := clientv3.New(etcdConfig)
	if err != nil {
		log.Fatal(err)
	}

	e := &etcd{
		etcdDB:     cli,
		namespaces: make(map[string]struct{}),
		lock:       sync.RWMutex{},
	}

	if err := e.CreateNamespace(ctx, defaultNamespace); err != nil {
		return nil, err
	}

	return e, nil
}

func etcdNamespaceKey(namespace string) string {
	return namespaceKey + "/" + namespace
}

// etcdPrefixRange is the key range holding the prefix resources of a
// namespace. Prefix keys are encoded as "<namespace>@<cidr>".
func etcdPrefixRange(namespace string) string {
	return namespace + "@"
}

// etcdTimeoutContext enforces the per-operation deadline consistent with the
// rest of the backend. A timeout aborts the local call before any cache
// mutation; etcd transactions themselves stay atomic.
func etcdTimeoutContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, 5*time.Second)
}

// readMarker returns the authoritative namespace marker: its prefix counter,
// modification revision and existence. canonical is false for legacy markers
// without a numeric counter (created by older versions); the counter is then
// derived from the actual prefix resources and callers normalize the marker
// before taking a decision.
func (e *etcd) readMarker(ctx context.Context, namespace string) (count int64, modRevision int64, exists bool, canonical bool, err error) {
	getCtx, cancel := etcdTimeoutContext(ctx)
	defer cancel()
	res, err := e.etcdDB.Get(getCtx, etcdNamespaceKey(namespace))
	if err != nil {
		return 0, 0, false, false, fmt.Errorf("unable to read namespace key: %w", err)
	}
	if res.Count == 0 {
		return 0, 0, false, true, nil
	}
	kv := res.Kvs[0]
	if parsed, parseErr := strconv.ParseInt(string(kv.Value), 10, 64); parseErr == nil && parsed >= 0 {
		return parsed, kv.ModRevision, true, true, nil
	}
	countCtx, countCancel := etcdTimeoutContext(ctx)
	defer countCancel()
	counted, err := e.etcdDB.Get(countCtx, etcdPrefixRange(namespace), clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		return 0, 0, false, false, fmt.Errorf("unable to count prefixes: %w", err)
	}
	return counted.Count, kv.ModRevision, true, false, nil
}

// normalizeMarker rewrites a legacy marker value to the canonical decimal
// prefix count with a compare-and-swap. It reports whether the marker still
// exists at the expected revision.
func (e *etcd) normalizeMarker(ctx context.Context, namespace string, count int64, modRevision int64) (bool, error) {
	txnCtx, cancel := etcdTimeoutContext(ctx)
	defer cancel()
	res, err := e.etcdDB.Txn(txnCtx).
		If(clientv3.Compare(clientv3.ModRevision(etcdNamespaceKey(namespace)), "=", modRevision)).
		Then(clientv3.OpPut(etcdNamespaceKey(namespace), strconv.FormatInt(count, 10))).
		Commit()
	if err != nil {
		return false, err
	}
	return res.Succeeded, nil
}

// This should ONLY be called when e.Lock() has been acquired
func (e *etcd) checkNamespaceExists(ctx context.Context, namespace string) error {
	if _, ok := e.namespaces[namespace]; ok {
		return nil
	}
	getCtx, cancel := etcdTimeoutContext(ctx)
	defer cancel()
	res, err := e.etcdDB.Get(getCtx, etcdNamespaceKey(namespace))
	if err != nil {
		return fmt.Errorf("unable to read namespace key: %w", err)
	}
	if res.Count == 0 {
		return ErrNamespaceDoesNotExist
	}
	e.namespaces[namespace] = struct{}{}
	return nil
}

func (e *etcd) CreatePrefix(ctx context.Context, prefix Prefix, namespace string) (Prefix, error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	pfx, err := prefix.toJSON()
	if err != nil {
		return Prefix{}, err
	}
	key := namespace + "@" + prefix.Cidr
	markerKey := etcdNamespaceKey(namespace)

	// CAS loop: prefix put and counter increment commit together, guarded by
	// the marker revision, so the transition is linearizable against the
	// delete transaction of any other instance.
	for attempt := 0; attempt < etcdCASRetries; attempt++ {
		count, modRevision, exists, _, err := e.readMarker(ctx, namespace)
		if err != nil {
			return Prefix{}, err
		}
		if !exists {
			delete(e.namespaces, namespace)
			return Prefix{}, ErrNamespaceDoesNotExist
		}

		getCtx, getCancel := etcdTimeoutContext(ctx)
		get, err := e.etcdDB.Get(getCtx, key)
		getCancel()
		if err != nil {
			return Prefix{}, fmt.Errorf("unable to read existing prefix:%v, error:%w", prefix, err)
		}
		if get.Count != 0 {
			return Prefix{}, fmt.Errorf("prefix already exists:%v", prefix)
		}

		txnCtx, txnCancel := etcdTimeoutContext(ctx)
		res, err := e.etcdDB.Txn(txnCtx).
			If(
				clientv3.Compare(clientv3.ModRevision(markerKey), "=", modRevision),
				clientv3.Compare(clientv3.CreateRevision(key), "=", 0),
			).
			Then(
				clientv3.OpPut(key, string(pfx)),
				clientv3.OpPut(markerKey, strconv.FormatInt(count+1, 10)),
			).
			Commit()
		txnCancel()
		if err != nil {
			return Prefix{}, fmt.Errorf("unable to create prefix:%v, error:%w", prefix, err)
		}
		if res.Succeeded {
			e.namespaces[namespace] = struct{}{}
			return prefix, nil
		}
		// Lost the marker or prefix race: re-read and let the next iteration
		// produce the authoritative outcome (not-exist vs. duplicate vs. retry).
	}
	return Prefix{}, fmt.Errorf("%w: unable to create prefix:%s", ErrOptimisticLockError, prefix.Cidr)
}

func (e *etcd) ReadPrefix(ctx context.Context, prefix string, namespace string) (Prefix, error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	if err := e.checkNamespaceExists(ctx, namespace); err != nil {
		return Prefix{}, err
	}

	ctx, cancel := etcdTimeoutContext(ctx)
	defer cancel()
	key := namespace + "@" + prefix
	get, err := e.etcdDB.Get(ctx, key)
	if err != nil {
		return Prefix{}, fmt.Errorf("unable to read data from ETCD error:%w", err)
	}

	if get.Count == 0 {
		return Prefix{}, fmt.Errorf("%w unable to read existing prefix:%v", ErrNotFound, prefix)
	}

	return fromJSON(get.Kvs[0].Value)
}

func (e *etcd) DeleteAllPrefixes(ctx context.Context, namespace string) error {
	e.lock.Lock()
	defer e.lock.Unlock()

	// Resource removal and counter reset are one transaction guarded by the
	// marker revision.
	for attempt := 0; attempt < etcdCASRetries; attempt++ {
		count, modRevision, exists, canonical, err := e.readMarker(ctx, namespace)
		if err != nil {
			return err
		}
		if !exists {
			delete(e.namespaces, namespace)
			return ErrNamespaceDoesNotExist
		}
		markerKey := etcdNamespaceKey(namespace)
		then := []clientv3.Op{clientv3.OpDelete(etcdPrefixRange(namespace), clientv3.WithPrefix())}
		if !canonical || count != 0 {
			then = append(then, clientv3.OpPut(markerKey, "0"))
		}
		txnCtx, cancel := etcdTimeoutContext(ctx)
		res, txnErr := e.etcdDB.Txn(txnCtx).
			If(clientv3.Compare(clientv3.ModRevision(markerKey), "=", modRevision)).
			Then(then...).
			Commit()
		cancel()
		if txnErr != nil {
			return fmt.Errorf("unable to delete all prefixes:%w", txnErr)
		}
		if res.Succeeded {
			return nil
		}
	}
	return fmt.Errorf("%w: unable to delete all prefixes in namespace:%s", ErrOptimisticLockError, namespace)
}

func (e *etcd) ReadAllPrefixes(ctx context.Context, namespace string) (Prefixes, error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	if err := e.checkNamespaceExists(ctx, namespace); err != nil {
		return nil, err
	}

	ctx, cancel := etcdTimeoutContext(ctx)
	defer cancel()
	defaultOpts := []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithKeysOnly(), clientv3.WithSerializable()}
	pfxs, err := e.etcdDB.Get(ctx, etcdPrefixRange(namespace), defaultOpts...)
	if err != nil {
		return nil, fmt.Errorf("unable to get all prefix cidrs:%w", err)
	}

	result := Prefixes{}
	for _, pfx := range pfxs.Kvs {
		v, err := e.etcdDB.Get(ctx, string(pfx.Key))
		if err != nil {
			return nil, err
		}
		if v.Count == 0 {
			// Key vanished concurrently (e.g. DeletePrefix); skip it.
			continue
		}
		pfx, err := fromJSON(v.Kvs[0].Value)
		if err != nil {
			return nil, err
		}
		result = append(result, pfx)
	}
	return result, nil
}

func (e *etcd) ReadAllPrefixCidrs(ctx context.Context, namespace string) ([]string, error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	if err := e.checkNamespaceExists(ctx, namespace); err != nil {
		return nil, err
	}

	allPrefix := []string{}
	ctx, cancel := etcdTimeoutContext(ctx)
	defer cancel()
	defaultOpts := []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithKeysOnly(), clientv3.WithSerializable()}
	pfxs, err := e.etcdDB.Get(ctx, etcdPrefixRange(namespace), defaultOpts...)
	if err != nil {
		return nil, fmt.Errorf("unable to get all prefix cidrs:%w", err)
	}

	for _, pfx := range pfxs.Kvs {
		v, err := e.etcdDB.Get(ctx, string(pfx.Key))
		if err != nil {
			return nil, err
		}
		if v.Count == 0 {
			continue
		}
		pfx, err := fromJSON(v.Kvs[0].Value)
		if err != nil {
			return nil, err
		}
		allPrefix = append(allPrefix, string(pfx.Cidr))
	}

	return allPrefix, nil
}
func (e *etcd) UpdatePrefix(ctx context.Context, prefix Prefix, namespace string) (Prefix, error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	if err := e.checkNamespaceExists(ctx, namespace); err != nil {
		return Prefix{}, err
	}

	oldVersion := prefix.version
	prefix.version = oldVersion + 1
	pn, err := prefix.toJSON()
	if err != nil {
		return Prefix{}, err
	}

	ctx, cancel := etcdTimeoutContext(ctx)
	defer cancel()
	key := namespace + "@" + prefix.Cidr
	p, err := e.etcdDB.Get(ctx, key)
	if err != nil {
		return Prefix{}, fmt.Errorf("unable to read cidrs from ETCD:%w", err)
	}

	if p.Count == 0 {
		// The prefix resource is gone; a concurrent namespace deletion may
		// have committed, so re-check the authoritative marker instead of
		// trusting the positive cache.
		if _, _, exists, _, markerErr := e.readMarker(ctx, namespace); markerErr == nil && !exists {
			delete(e.namespaces, namespace)
			return Prefix{}, ErrNamespaceDoesNotExist
		}
		return Prefix{}, fmt.Errorf("unable to get all prefix cidrs:%w", ErrNotFound)
	}

	oldPrefix, err := fromJSON([]byte(p.Kvs[0].Value))
	if err != nil {
		return Prefix{}, err
	}

	// Actual operation (local in optimistic lock).
	if oldPrefix.version != oldVersion {
		return Prefix{}, fmt.Errorf("%w: unable to update prefix:%s", ErrOptimisticLockError, prefix.Cidr)
	}

	// Operation is committed only if the watched keys remain unchanged.
	_, err = e.etcdDB.Put(ctx, key, string(pn))
	if err != nil {
		return Prefix{}, fmt.Errorf("unable to update prefix:%s, error:%w", prefix.Cidr, err)
	}

	return prefix, nil
}
func (e *etcd) DeletePrefix(ctx context.Context, prefix Prefix, namespace string) (Prefix, error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	if err := e.checkNamespaceExists(ctx, namespace); err != nil {
		return Prefix{}, err
	}

	key := namespace + "@" + prefix.Cidr
	markerKey := etcdNamespaceKey(namespace)

	// Decrement counter and remove resource in a single CAS. A missing
	// prefix key keeps the legacy no-op success semantics without touching
	// the counter.
	for attempt := 0; attempt < etcdCASRetries; attempt++ {
		count, modRevision, exists, canonical, err := e.readMarker(ctx, namespace)
		if err != nil {
			return Prefix{}, err
		}
		if !exists {
			delete(e.namespaces, namespace)
			return Prefix{}, ErrNamespaceDoesNotExist
		}

		getCtx, getCancel := etcdTimeoutContext(ctx)
		get, err := e.etcdDB.Get(getCtx, key)
		getCancel()
		if err != nil {
			return Prefix{}, err
		}
		if get.Count == 0 {
			// Legacy semantics: deleting an unknown prefix succeeds.
			return *prefix.deepCopy(), nil
		}

		var thenOps []clientv3.Op
		if canonical && count > 0 {
			thenOps = append(thenOps,
				clientv3.OpDelete(key),
				clientv3.OpPut(markerKey, strconv.FormatInt(count-1, 10)),
			)
		} else {
			// Legacy/non-canonical marker or counter at zero for an existing
			// resource: only delete the key and let later operations
			// reconcile the counter from the real resources.
			thenOps = append(thenOps, clientv3.OpDelete(key))
		}
		txnCtx, txnCancel := etcdTimeoutContext(ctx)
		res, txnErr := e.etcdDB.Txn(txnCtx).
			If(clientv3.Compare(clientv3.ModRevision(markerKey), "=", modRevision)).
			Then(thenOps...).
			Commit()
		txnCancel()
		if txnErr != nil {
			return *prefix.deepCopy(), txnErr
		}
		if res.Succeeded {
			return *prefix.deepCopy(), nil
		}
	}
	return *prefix.deepCopy(), fmt.Errorf("%w: unable to delete prefix:%s", ErrOptimisticLockError, prefix.Cidr)
}

func (e *etcd) CreateNamespace(ctx context.Context, namespace string) error {
	e.lock.Lock()
	defer e.lock.Unlock()

	// Idempotent: put the marker with counter 0 only when it does not exist,
	// so recreating a namespace never resets an in-use counter.
	txnCtx, cancel := etcdTimeoutContext(ctx)
	_, err := e.etcdDB.Txn(txnCtx).
		If(clientv3.Compare(clientv3.CreateRevision(etcdNamespaceKey(namespace)), "=", 0)).
		Then(clientv3.OpPut(etcdNamespaceKey(namespace), "0")).
		Commit()
	cancel()
	if err == nil {
		e.namespaces[namespace] = struct{}{}
	}
	return err
}

func (e *etcd) ListNamespaces(ctx context.Context) ([]string, error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	key := etcdNamespaceKey("")
	defaultOpts := []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithKeysOnly(), clientv3.WithSerializable()}
	ctx, cancel := etcdTimeoutContext(ctx)
	defer cancel()
	res, err := e.etcdDB.Get(ctx, key, defaultOpts...)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(res.Kvs))
	for _, kv := range res.Kvs {
		result = append(result, strings.TrimPrefix(string(kv.Key), key))
	}
	return result, nil
}

// DeleteNamespace is the atomic lifecycle transition: it commits only when
// the marker still declares zero prefixes, and in the same transaction removes
// the namespace directory marker and all of its prefix resources. A create
// committing first moves the counter to 1 so the transaction fails with
// ErrNamespaceNotEmpty and every resource is retained; a delete committing
// first removes the marker, so every later create observes
// ErrNamespaceDoesNotExist until CreateNamespace rebuilds the namespace.
func (e *etcd) DeleteNamespace(ctx context.Context, namespace string) error {
	e.lock.Lock()
	defer e.lock.Unlock()

	markerKey := etcdNamespaceKey(namespace)

	for attempt := 0; attempt < etcdCASRetries; attempt++ {
		count, modRevision, exists, canonical, err := e.readMarker(ctx, namespace)
		if err != nil {
			return err
		}
		if !exists {
			delete(e.namespaces, namespace)
			return ErrNamespaceDoesNotExist
		}
		if count > 0 {
			// Create-wins conflict: nothing is deleted.
			return errNamespaceHasPrefixes
		}
		if !canonical {
			// Legacy empty marker: canonicalize it, then re-decide.
			normalized, err := e.normalizeMarker(ctx, namespace, 0, modRevision)
			if err != nil {
				return fmt.Errorf("unable to normalize namespace marker:%s, error:%w", namespace, err)
			}
			if !normalized {
				// Marker changed concurrently: re-read.
				continue
			}
			continue
		}

		txnCtx, cancel := etcdTimeoutContext(ctx)
		res, txnErr := e.etcdDB.Txn(txnCtx).
			If(
				clientv3.Compare(clientv3.ModRevision(markerKey), "=", modRevision),
				clientv3.Compare(clientv3.Value(markerKey), "=", "0"),
			).
			Then(
				clientv3.OpDelete(markerKey),
				clientv3.OpDelete(etcdPrefixRange(namespace), clientv3.WithPrefix()),
			).
			Commit()
		cancel()
		if txnErr != nil {
			return fmt.Errorf("unable to delete namespace:%s, error:%w", namespace, txnErr)
		}
		if res.Succeeded {
			delete(e.namespaces, namespace)
			return nil
		}
		// Lost a race against another marker writer; re-read and let the next
		// iteration decide between conflict, absence and retry.
	}
	return fmt.Errorf("%w: unable to delete namespace:%s", ErrOptimisticLockError, namespace)
}
