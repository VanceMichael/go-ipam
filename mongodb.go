package ipam

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const dbCidr = `prefix.cidr`
const versionKey = `version`

// systemDatabaseSuffix names a separate database holding the namespace
// registry. A separate database guarantees that a tenant namespace can never
// collide with the registry collection regardless of its name.
const systemDatabaseSuffix = `__ipam_system`

// registryCollection holds one document per namespace:
//
//	{"_id": <namespace>, "prefixCount": <int64>}
//
// The registry is the linearization point for create/delete transitions: its
// documents are modified with atomic find-one-and-update/delete operations,
// which works on standalone mongod as well as on replica sets.
const registryCollection = "namespaces"

type MongoConfig struct {
	DatabaseName       string
	MongoClientOptions *options.ClientOptions
}

type mongodb struct {
	db         *mongo.Database
	sys        *mongo.Database
	namespaces map[string]struct{}
	lock       sync.RWMutex
}

func NewMongo(ctx context.Context, config MongoConfig) (Storage, error) {
	return newMongo(ctx, config)
}

func (m *mongodb) Name() string {
	return "mongodb"
}

func newMongo(ctx context.Context, config MongoConfig) (*mongodb, error) {
	client, err := mongo.Connect(ctx, config.MongoClientOptions)
	if err != nil {
		return nil, err
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}

	db := &mongodb{
		db:         client.Database(config.DatabaseName),
		sys:        client.Database(config.DatabaseName + systemDatabaseSuffix),
		namespaces: make(map[string]struct{}),
		lock:       sync.RWMutex{},
	}
	if err := db.CreateNamespace(ctx, defaultNamespace); err != nil {
		return nil, err
	}
	return db, nil
}

func (m *mongodb) registry() *mongo.Collection {
	return m.sys.Collection(registryCollection)
}

// checkNamespaceExists consults the authoritative registry rather than the
// list of physical collections: an empty collection left behind by a failed
// Drop must not resurrect a deleted namespace.
func (m *mongodb) checkNamespaceExists(ctx context.Context, namespace string) error {
	if _, ok := m.namespaces[namespace]; ok {
		return nil
	}

	count, err := m.registry().CountDocuments(ctx, bson.D{{Key: "_id", Value: namespace}})
	if err != nil {
		return fmt.Errorf("error checking namespace: %w", err)
	}
	if count == 0 {
		return ErrNamespaceDoesNotExist
	}

	m.namespaces[namespace] = struct{}{}
	return nil
}

// checkNamespaceExistsNow bypasses the positive local cache and asks the
// registry directly, evicting a stale cache entry when the namespace is gone.
func (m *mongodb) checkNamespaceExistsNow(ctx context.Context, namespace string) error {
	count, err := m.registry().CountDocuments(ctx, bson.D{{Key: "_id", Value: namespace}})
	if err != nil {
		return fmt.Errorf("error checking namespace: %w", err)
	}
	if count == 0 {
		delete(m.namespaces, namespace)
		return ErrNamespaceDoesNotExist
	}
	return nil
}

func (m *mongodb) CreatePrefix(ctx context.Context, prefix Prefix, namespace string) (Prefix, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	collection := m.db.Collection(namespace)

	// Duplicate prefixes are rejected before reserving counter capacity.
	f := bson.D{{Key: dbCidr, Value: prefix.Cidr}}
	r := collection.FindOne(ctx, f)
	if r.Err() == nil {
		return Prefix{}, fmt.Errorf("prefix already exists:%s", prefix.Cidr)
	} else if r.Err() != nil && !errors.Is(r.Err(), mongo.ErrNoDocuments) {
		return Prefix{}, fmt.Errorf("unable to read prefix:%s, error:%w", prefix.Cidr, r.Err())
	}

	// Reserve one slot atomically. When the registry document is gone the
	// transition has been won by DeleteNamespace and creation fails without
	// leaving any resource behind.
	reserve := m.registry().FindOneAndUpdate(
		ctx,
		bson.D{{Key: "_id", Value: namespace}},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "prefixCount", Value: 1}}}},
		options.FindOneAndUpdate().SetReturnDocument(options.Before),
	)
	if reserveErr := reserve.Err(); reserveErr != nil {
		if errors.Is(reserveErr, mongo.ErrNoDocuments) {
			delete(m.namespaces, namespace)
			return Prefix{}, ErrNamespaceDoesNotExist
		}
		return Prefix{}, fmt.Errorf("unable to reserve prefix slot:%s, error:%w", prefix.Cidr, reserveErr)
	}

	// Release a previously reserved slot after a failed insert.
	compensate := func(cause error) (Prefix, error) {
		_, compErr := m.registry().UpdateOne(ctx,
			bson.D{{Key: "_id", Value: namespace}},
			bson.D{{Key: "$inc", Value: bson.D{{Key: "prefixCount", Value: -1}}}},
		)
		if compErr != nil {
			return Prefix{}, fmt.Errorf("%w (counter compensation also failed: %v)", cause, compErr)
		}
		return Prefix{}, cause
	}

	_, err := collection.InsertOne(ctx, prefix.toPrefixJSON())
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return compensate(fmt.Errorf("prefix already exists:%s", prefix.Cidr))
		}
		return compensate(fmt.Errorf("unable to insert prefix:%s, error:%w", prefix.Cidr, err))
	}

	return prefix, nil
}

func (m *mongodb) ReadPrefix(ctx context.Context, prefix string, namespace string) (Prefix, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if err := m.checkNamespaceExists(ctx, namespace); err != nil {
		return Prefix{}, err
	}

	f := bson.D{{Key: dbCidr, Value: prefix}}
	r := m.db.Collection(namespace).FindOne(ctx, f)

	// ErrNoDocuments should be returned if the prefix does not exist
	if r.Err() != nil && errors.Is(r.Err(), mongo.ErrNoDocuments) {
		return Prefix{}, fmt.Errorf(`%w prefix not found:%s, error:%w`, ErrNotFound, prefix, r.Err())
	} else if r.Err() != nil {
		return Prefix{}, fmt.Errorf(`error while trying to find prefix:%s, error:%w`, prefix, r.Err())
	}

	j := prefixJSON{}
	err := r.Decode(&j)
	if err != nil {
		return Prefix{}, fmt.Errorf("unable to read prefix:%w", err)
	}
	return j.toPrefix(), nil
}

func (m *mongodb) DeleteAllPrefixes(ctx context.Context, namespace string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	if err := m.checkNamespaceExists(ctx, namespace); err != nil {
		return err
	}

	res, err := m.db.Collection(namespace).DeleteMany(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf(`error deleting all prefixes: %w`, err)
	}
	if res.DeletedCount > 0 {
		_, err = m.registry().UpdateOne(ctx,
			bson.D{{Key: "_id", Value: namespace}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "prefixCount", Value: 0}}}},
		)
		if err != nil {
			return fmt.Errorf("error resetting prefix counter: %w", err)
		}
	}
	return nil
}

func (m *mongodb) ReadAllPrefixes(ctx context.Context, namespace string) (Prefixes, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if err := m.checkNamespaceExists(ctx, namespace); err != nil {
		return nil, err
	}

	f := bson.D{} // match all documents
	c, err := m.db.Collection(namespace).Find(ctx, f)
	if err != nil {
		return nil, fmt.Errorf(`error reading all prefixes: %w`, err)
	}
	var r []prefixJSON
	if err := c.All(ctx, &r); err != nil {
		return nil, fmt.Errorf(`error reading all prefixes: %w`, err)
	}

	s := make([]Prefix, len(r))
	for i, v := range r {
		s[i] = v.toPrefix()
	}

	return s, nil
}

func (m *mongodb) ReadPrefixes(ctx context.Context, namespace string) (Prefixes, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if err := m.checkNamespaceExists(ctx, namespace); err != nil {
		return nil, err
	}

	f := bson.D{} // match all documents
	c, err := m.db.Collection(namespace).Find(ctx, f)
	if err != nil {
		return nil, fmt.Errorf(`error reading all prefixes: %w`, err)
	}
	var r []prefixJSON
	if err := c.All(ctx, &r); err != nil {
		return nil, fmt.Errorf(`error reading all prefixes: %w`, err)
	}

	s := make([]Prefix, len(r))
	for i, v := range r {
		s[i] = v.toPrefix()
	}

	return s, nil
}

func (m *mongodb) ReadAllPrefixCidrs(ctx context.Context, namespace string) ([]string, error) {
	p, err := m.ReadAllPrefixes(ctx, namespace)
	if err != nil {
		return nil, err
	}
	s := make([]string, len(p))
	for i, v := range p {
		s[i] = v.Cidr
	}
	return s, nil
}

func (m *mongodb) UpdatePrefix(ctx context.Context, prefix Prefix, namespace string) (Prefix, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if err := m.checkNamespaceExists(ctx, namespace); err != nil {
		return Prefix{}, err
	}

	oldVersion := prefix.version
	prefix.version = oldVersion + 1

	f := bson.D{{Key: dbCidr, Value: prefix.Cidr}, {Key: versionKey, Value: oldVersion}}

	o := options.Replace().SetUpsert(false)
	r, err := m.db.Collection(namespace).ReplaceOne(ctx, f, prefix.toPrefixJSON(), o)
	if err != nil {
		return Prefix{}, fmt.Errorf("unable to update prefix:%s, error: %w", prefix.Cidr, err)
	}
	if r.MatchedCount == 0 {
		// Either the prefix vanished (a concurrent delete) or its version
		// moved; distinguish the namespace lifecycle outcome authoritatively.
		if existsErr := m.checkNamespaceExistsNow(ctx, namespace); errors.Is(existsErr, ErrNamespaceDoesNotExist) {
			return Prefix{}, ErrNamespaceDoesNotExist
		}
		return Prefix{}, fmt.Errorf("%w: unable to update prefix:%s", ErrOptimisticLockError, prefix.Cidr)
	}
	if r.ModifiedCount == 0 {
		return Prefix{}, fmt.Errorf("%w: update did not effect any document:%s",
			ErrOptimisticLockError, prefix.Cidr)
	}

	return prefix, nil
}

func (m *mongodb) DeletePrefix(ctx context.Context, prefix Prefix, namespace string) (Prefix, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if err := m.checkNamespaceExists(ctx, namespace); err != nil {
		return Prefix{}, err
	}

	f := bson.D{{Key: dbCidr, Value: prefix.Cidr}}
	r := m.db.Collection(namespace).FindOneAndDelete(ctx, f)

	// ErrNoDocuments should be returned if the prefix does not exist
	if r.Err() != nil && errors.Is(r.Err(), mongo.ErrNoDocuments) {
		return Prefix{}, fmt.Errorf(`prefix not found:%s, error:%w`, prefix.Cidr, r.Err())
	} else if r.Err() != nil {
		return Prefix{}, fmt.Errorf(`error while trying to find prefix:%s, error:%w`, prefix.Cidr, r.Err())
	}

	j := prefixJSON{}
	if err := r.Decode(&j); err != nil {
		return Prefix{}, fmt.Errorf("unable to read prefix:%w", err)
	}
	// One resource was physically removed: release its counter slot.
	if _, err := m.registry().UpdateOne(ctx,
		bson.D{{Key: "_id", Value: namespace}},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "prefixCount", Value: -1}}}},
	); err != nil {
		return Prefix{}, fmt.Errorf("error releasing prefix counter: %w", err)
	}
	return j.toPrefix(), nil
}

func (m *mongodb) CreateNamespace(ctx context.Context, namespace string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	// Register the namespace directory atomically. The _id uniqueness makes
	// this idempotent; a recreated namespace starts with a fresh zero counter.
	registered := true
	if _, err := m.registry().InsertOne(ctx, bson.D{
		{Key: "_id", Value: namespace},
		{Key: "prefixCount", Value: int64(0)},
	}); err != nil {
		if !mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("unable to register namespace: %w", err)
		}
		registered = false
	}

	if err := m.db.CreateCollection(ctx, namespace); err != nil {
		var e mongo.CommandError
		if !(errors.As(err, &e) && e.Name == "NamespaceExists") {
			return err
		}
	}
	if _, err := m.db.Collection(namespace).Indexes().CreateMany(ctx, []mongo.IndexModel{{
		Keys:    bson.D{{Key: dbCidr, Value: 1}},
		Options: options.Index().SetUnique(true),
	}}); err != nil {
		return err
	}

	if registered {
		// The directory row was created just now. When upgrading databases
		// written by older versions the physical collection may already hold
		// prefixes, so backfill the counter from reality once instead of
		// trusting the zero value.
		count, err := m.db.Collection(namespace).CountDocuments(ctx, bson.D{})
		if err != nil {
			return fmt.Errorf("unable to backfill prefix counter: %w", err)
		}
		if count > 0 {
			if _, err := m.registry().UpdateOne(ctx,
				bson.D{{Key: "_id", Value: namespace}},
				bson.D{{Key: "$set", Value: bson.D{{Key: "prefixCount", Value: count}}}},
			); err != nil {
				return fmt.Errorf("unable to backfill prefix counter: %w", err)
			}
		}
	}

	m.namespaces[namespace] = struct{}{}
	return nil
}

func (m *mongodb) ListNamespaces(ctx context.Context) ([]string, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	c, err := m.registry().Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	var docs []struct {
		ID string `bson:"_id"`
	}
	if err := c.All(ctx, &docs); err != nil {
		return nil, err
	}
	// Reconcile the local cache with the authoritative registry, including
	// namespaces removed by other instances.
	fresh := make(map[string]struct{}, len(docs))
	result := make([]string, 0, len(docs))
	for _, doc := range docs {
		fresh[doc.ID] = struct{}{}
		result = append(result, doc.ID)
	}
	m.namespaces = fresh
	return result, nil
}

// DeleteNamespace is the atomic lifecycle transition. The registry document
// is removed with a findOneAndDelete guarded on prefixCount == 0: that single
// atomic operation is the commit point and can never succeed for a namespace
// in which a prefix creation has committed first. After committing, the
// physical prefix collection is dropped best-effort; if that physical removal
// fails the namespace is still observably gone (registry and caches agree),
// and it is transparently recreated on an explicit CreateNamespace.
func (m *mongodb) DeleteNamespace(ctx context.Context, namespace string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	res := m.registry().FindOneAndDelete(ctx, bson.D{
		{Key: "_id", Value: namespace},
		{Key: "prefixCount", Value: 0},
	})
	err := res.Err()
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return fmt.Errorf("unable to delete namespace:%s, error:%w", namespace, err)
		}
		// The guarded delete matched nothing: distinguish not-exist from
		// create-wins (non-empty).
		probe := m.registry().FindOne(ctx, bson.D{{Key: "_id", Value: namespace}})
		if probe.Err() != nil {
			if errors.Is(probe.Err(), mongo.ErrNoDocuments) {
				delete(m.namespaces, namespace)
				return ErrNamespaceDoesNotExist
			}
			return fmt.Errorf("unable to inspect namespace:%s, error:%w", namespace, probe.Err())
		}
		return errNamespaceHasPrefixes
	}

	// Commit observed: directory removed. Drop the physical resource; the
	// registry remains the source of truth even when this fails.
	if dropErr := m.db.Collection(namespace).Drop(ctx); dropErr != nil {
		delete(m.namespaces, namespace)
		return fmt.Errorf("namespace %s deleted but physical collection drop failed: %w", namespace, dropErr)
	}
	delete(m.namespaces, namespace)
	return nil
}
