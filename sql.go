package ipam

import (
	"context"
	dbsql "database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// namespaceRegistryTable is the authoritative namespace directory. A row
// exists exactly while the namespace is alive; prefix_count carries the number
// of prefix resources. The row is the linearization point taken in FOR UPDATE
// mode for create/delete transitions, which works identically on PostgreSQL
// and CockroachDB (unlike DDL-only transactions).
const namespaceRegistryTable = "namespaces"

type sql struct {
	db          *sqlx.DB
	maxIdLength int
	tables      sync.Map
}

// sqlMaxTxRetries bounds retries of serialization/deadlock failures.
const sqlMaxTxRetries = 10

// errSQLPrefixDuplicate is the in-transaction signal for a unique cidr
// collision; callers translate it to the historical "return existing prefix"
// behavior.
var errSQLPrefixDuplicate = errors.New("prefix already exists")

// isUndefinedTable reports whether the error is PostgreSQL's undefined_table
// (42P01), i.e. a namespace table that was dropped by a committed
// DeleteNamespace.
func isUndefinedTable(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "42P01"
}

// isUniqueViolation reports PostgreSQL's unique_violation (23505).
func isUniqueViolation(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "23505"
}

// isRetryableTxn reports a serialization/deadlock failure that is safe to
// retry (CockroachDB 40001, PostgreSQL 40P01).
func isRetryableTxn(err error) bool {
	var pqErr *pq.Error
	if !errors.As(err, &pqErr) {
		return false
	}
	return pqErr.Code == "40001" || pqErr.Code == "40P01"
}

// runTx executes fn inside a transaction, rolling back on error and retrying
// the whole transaction on serialization/deadlock failures. Sentinels
// returned by fn are never retried.
func (s *sql) runTx(ctx context.Context, fn func(tx *sqlx.Tx) error) error {
	var lastErr error
	for attempt := 0; attempt < sqlMaxTxRetries; attempt++ {
		tx, err := s.db.Beginx()
		if err != nil {
			return fmt.Errorf("unable to start transaction:%w", err)
		}
		err = fn(tx)
		if err != nil {
			_ = tx.Rollback()
			if isRetryableTxn(err) {
				lastErr = err
				continue
			}
			return err
		}
		if err := tx.Commit(); err != nil {
			if isRetryableTxn(err) {
				lastErr = err
				continue
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("%w: transaction did not converge after retries: %v", ErrOptimisticLockError, lastErr)
}

// lockNamespaceRow takes the transaction-local FOR UPDATE lock on the
// namespace directory row. It returns ErrNamespaceDoesNotExist when the
// namespace transitioned away concurrently.
func lockNamespaceRow(ctx context.Context, tx *sqlx.Tx, namespace string) error {
	var count int64
	err := tx.GetContext(ctx, &count,
		"SELECT prefix_count FROM "+namespaceRegistryTable+" WHERE name = $1 FOR UPDATE",
		namespace,
	)
	if errors.Is(err, dbsql.ErrNoRows) {
		return ErrNamespaceDoesNotExist
	}
	if err != nil {
		if isUndefinedTable(err) {
			return ErrNamespaceDoesNotExist
		}
		return fmt.Errorf("unable to lock namespace row:%w", err)
	}
	return nil
}

func createTableSQL(namespace string) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
	cidr   text PRIMARY KEY NOT NULL,
	prefix JSONB
);
CREATE INDEX IF NOT EXISTS prefix_idx ON %s USING GIN(prefix);
CREATE TABLE IF NOT EXISTS %s (
	name         text PRIMARY KEY NOT NULL,
	prefix_count bigint NOT NULL
);
`, getTableName(namespace), getTableName(namespace), namespaceRegistryTable)
}

func getTableName(namespace string) string {
	if namespace == defaultNamespace {
		return "prefixes"
	}
	return fmt.Sprintf("\"prefixes_%s\"", namespace)
}

func (s *sql) prefixExists(ctx context.Context, prefix Prefix, namespace string) (*Prefix, bool) {
	p, err := s.ReadPrefix(ctx, prefix.Cidr, namespace)
	if err != nil {
		return nil, false
	}
	return &p, true
}

func (s *sql) checkNamespaceExists(ctx context.Context, namespace string) error {
	if _, ok := s.tables.Load(namespace); ok {
		return nil
	}
	// populate namespaces from sql
	if _, err := s.ListNamespaces(ctx); err != nil {
		return err
	}
	if _, ok := s.tables.Load(namespace); !ok {
		return ErrNamespaceDoesNotExist
	}
	return nil
}

func (s *sql) CreatePrefix(ctx context.Context, prefix Prefix, namespace string) (Prefix, error) {
	if err := s.checkNamespaceExists(ctx, namespace); err != nil {
		return Prefix{}, err
	}
	existingPrefix, exists := s.prefixExists(ctx, prefix, namespace)
	if exists {
		return *existingPrefix, nil
	}
	prefix.version = int64(0)
	pj, err := prefix.toJSON()
	if err != nil {
		return Prefix{}, err
	}

	err = s.runTx(ctx, func(tx *sqlx.Tx) error {
		if err := lockNamespaceRow(ctx, tx, namespace); err != nil {
			return err
		}
		res, execErr := tx.ExecContext(ctx, "INSERT INTO "+getTableName(namespace)+"(cidr, prefix) VALUES ($1, $2)", prefix.Cidr, pj)
		if execErr != nil {
			if isUndefinedTable(execErr) {
				return ErrNamespaceDoesNotExist
			}
			if isUniqueViolation(execErr) {
				return errSQLPrefixDuplicate
			}
			return fmt.Errorf("unable to insert prefix:%w", execErr)
		}
		if affected, _ := res.RowsAffected(); affected > 0 {
			if _, execErr = tx.ExecContext(ctx,
				"UPDATE "+namespaceRegistryTable+" SET prefix_count = prefix_count + 1 WHERE name = $1",
				namespace,
			); execErr != nil {
				return fmt.Errorf("unable to increment prefix counter:%w", execErr)
			}
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrNamespaceDoesNotExist) {
			s.tables.Delete(namespace)
			return Prefix{}, ErrNamespaceDoesNotExist
		}
		if errors.Is(err, errSQLPrefixDuplicate) {
			if existing, ok := s.prefixExists(ctx, prefix, namespace); ok {
				return *existing, nil
			}
			return Prefix{}, errSQLPrefixDuplicate
		}
		return Prefix{}, err
	}
	return prefix, nil
}

func (s *sql) ReadPrefix(ctx context.Context, prefix, namespace string) (Prefix, error) {
	if err := s.checkNamespaceExists(ctx, namespace); err != nil {
		return Prefix{}, err
	}
	var result []byte
	err := s.db.GetContext(ctx, &result, "SELECT prefix FROM "+getTableName(namespace)+" WHERE cidr=$1", prefix)
	if err != nil {
		if errors.Is(err, dbsql.ErrNoRows) {
			return Prefix{}, fmt.Errorf("%w prefix:%s not found:%s", ErrNotFound, prefix, err.Error())
		}
		if isUndefinedTable(err) {
			s.tables.Delete(namespace)
			return Prefix{}, ErrNamespaceDoesNotExist
		}
		return Prefix{}, fmt.Errorf("unable to read prefix:%w", err)
	}
	return fromJSON(result)
}

func (s *sql) DeleteAllPrefixes(ctx context.Context, namespace string) error {
	if err := s.checkNamespaceExists(ctx, namespace); err != nil {
		return err
	}
	return s.runTx(ctx, func(tx *sqlx.Tx) error {
		if err := lockNamespaceRow(ctx, tx, namespace); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+getTableName(namespace)); err != nil {
			if isUndefinedTable(err) {
				return ErrNamespaceDoesNotExist
			}
			return err
		}
		_, err := tx.ExecContext(ctx,
			"UPDATE "+namespaceRegistryTable+" SET prefix_count = 0 WHERE name = $1",
			namespace,
		)
		return err
	})
}

// ReadAllPrefixes returns all known prefixes.
func (s *sql) ReadAllPrefixes(ctx context.Context, namespace string) (Prefixes, error) {
	if err := s.checkNamespaceExists(ctx, namespace); err != nil {
		return nil, err
	}
	var prefixes [][]byte
	err := s.db.SelectContext(ctx, &prefixes, "SELECT prefix FROM "+getTableName(namespace))
	if err != nil {
		if isUndefinedTable(err) {
			s.tables.Delete(namespace)
			return nil, ErrNamespaceDoesNotExist
		}
		return nil, fmt.Errorf("unable to read prefixes:%w", err)
	}
	return toPrefixes(prefixes)
}

func toPrefixes(prefixes [][]byte) ([]Prefix, error) {
	result := Prefixes{}
	for _, v := range prefixes {
		pfx, err := fromJSON(v)
		if err != nil {
			return nil, err
		}
		result = append(result, pfx)
	}
	return result, nil
}

// ReadAllPrefixCidrs is cheaper that ReadAllPrefixes because it only returns the Cidrs.
func (s *sql) ReadAllPrefixCidrs(ctx context.Context, namespace string) ([]string, error) {
	if err := s.checkNamespaceExists(ctx, namespace); err != nil {
		return nil, err
	}
	cidrs := []string{}
	err := s.db.SelectContext(ctx, &cidrs, "SELECT cidr FROM "+getTableName(namespace))
	if err != nil {
		if isUndefinedTable(err) {
			s.tables.Delete(namespace)
			return nil, ErrNamespaceDoesNotExist
		}
		return nil, fmt.Errorf("unable to read prefixes in namespace:%s :%w", namespace, err)
	}
	return cidrs, nil
}

// UpdatePrefix tries to update the prefix.
// Returns OptimisticLockError if it does not succeed due to a concurrent update.
func (s *sql) UpdatePrefix(ctx context.Context, prefix Prefix, namespace string) (Prefix, error) {
	if err := s.checkNamespaceExists(ctx, namespace); err != nil {
		return Prefix{}, err
	}
	oldVersion := prefix.version
	prefix.version = oldVersion + 1
	pn, err := prefix.toJSON()
	if err != nil {
		return Prefix{}, err
	}
	tx, err := s.db.Beginx()
	if err != nil {
		return Prefix{}, fmt.Errorf("unable to start transaction:%w", err)
	}
	// Defer a rollback in case anything fails.
	defer func() {
		_ = tx.Rollback()
	}()

	result, err := tx.ExecContext(ctx, "SELECT prefix FROM "+getTableName(namespace)+" WHERE cidr=$1 AND prefix->>'Version'=$2 FOR UPDATE", prefix.Cidr, oldVersion)
	if err != nil {
		if isUndefinedTable(err) {
			s.tables.Delete(namespace)
			return Prefix{}, ErrNamespaceDoesNotExist
		}
		return Prefix{}, fmt.Errorf("%w: unable to select for update prefix:%s", ErrOptimisticLockError, prefix.Cidr)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return Prefix{}, err
	}
	if rows == 0 {
		// Rollback, but ignore error, if rollback is omitted, the row lock created by SELECT FOR UPDATE will not get released.
		_ = tx.Rollback()
		return Prefix{}, fmt.Errorf("%w: select for update did not effect any row", ErrOptimisticLockError)
	}
	result, err = tx.ExecContext(ctx, "UPDATE "+getTableName(namespace)+" SET prefix=$1 WHERE cidr=$2 AND prefix->>'Version'=$3", pn, prefix.Cidr, oldVersion)
	if err != nil {
		return Prefix{}, fmt.Errorf("%w: unable to update prefix:%s", ErrOptimisticLockError, prefix.Cidr)
	}
	rows, err = result.RowsAffected()
	if err != nil {
		return Prefix{}, err
	}
	if rows == 0 {
		// Rollback, but ignore error, if rollback is omitted, the row lock created by SELECT FOR UPDATE will not get released.
		_ = tx.Rollback()
		return Prefix{}, fmt.Errorf("%w: updatePrefix did not effect any row", ErrOptimisticLockError)
	}
	if err := tx.Commit(); err != nil {
		return Prefix{}, err
	}
	return prefix, nil
}

func (s *sql) DeletePrefix(ctx context.Context, prefix Prefix, namespace string) (Prefix, error) {
	if err := s.checkNamespaceExists(ctx, namespace); err != nil {
		return Prefix{}, err
	}
	err := s.runTx(ctx, func(tx *sqlx.Tx) error {
		if err := lockNamespaceRow(ctx, tx, namespace); err != nil {
			return err
		}
		res, execErr := tx.ExecContext(ctx, "DELETE FROM "+getTableName(namespace)+" WHERE cidr=$1", prefix.Cidr)
		if execErr != nil {
			if isUndefinedTable(execErr) {
				return ErrNamespaceDoesNotExist
			}
			return fmt.Errorf("unable delete prefix: %w", execErr)
		}
		if affected, _ := res.RowsAffected(); affected > 0 {
			if _, execErr = tx.ExecContext(ctx,
				"UPDATE "+namespaceRegistryTable+" SET prefix_count = prefix_count - 1 WHERE name = $1 AND prefix_count > 0",
				namespace,
			); execErr != nil {
				return fmt.Errorf("unable to decrement prefix counter: %w", execErr)
			}
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrNamespaceDoesNotExist) {
			s.tables.Delete(namespace)
			return Prefix{}, ErrNamespaceDoesNotExist
		}
		return Prefix{}, err
	}
	return prefix, nil
}
func (s *sql) Name() string {
	return "postgres"
}

func (s *sql) CreateNamespace(ctx context.Context, namespace string) error {
	if len(namespace) > s.maxIdLength {
		return ErrNameTooLong
	}
	if _, ok := s.tables.Load(namespace); !ok {
		if _, err := s.db.ExecContext(ctx, createTableSQL(namespace)); err != nil {
			return fmt.Errorf("unable to create table: %w", err)
		}
		// Register the namespace directory. A freshly created prefix table is
		// always empty, so the registration is idempotent and never resets a
		// counter of a namespace that was registered before.
		if _, err := s.db.ExecContext(ctx,
			"INSERT INTO "+namespaceRegistryTable+"(name, prefix_count) VALUES ($1, 0) ON CONFLICT (name) DO NOTHING",
			namespace,
		); err != nil {
			return fmt.Errorf("unable to register namespace: %w", err)
		}
		s.tables.Store(namespace, struct{}{})
	}
	return nil
}

func (s *sql) ListNamespaces(ctx context.Context) ([]string, error) {
	var result []string
	if err := s.db.SelectContext(ctx, &result, "SELECT name FROM "+namespaceRegistryTable); err != nil {
		if isUndefinedTable(err) {
			// Registry table does not exist yet (no namespace was created).
			return nil, nil
		}
		return nil, fmt.Errorf("unable to list namespaces: %w", err)
	}
	for i := range result {
		s.tables.Store(result[i], struct{}{})
	}
	return result, nil
}

// DeleteNamespace is the atomic lifecycle transition. Inside one transaction
// it locks the namespace directory row FOR UPDATE, checks the authoritative
// prefix counter, removes the directory row and drops the physical prefix
// table. A concurrent create is serialized on the same row lock: it either
// increments the counter first (non-empty conflict, everything retained) or
// arrives after the row is gone (ErrNamespaceDoesNotExist until an explicit
// CreateNamespace rebuild).
func (s *sql) DeleteNamespace(ctx context.Context, namespace string) error {
	if err := s.checkNamespaceExists(ctx, namespace); err != nil {
		return err
	}
	table := getTableName(namespace)
	err := s.runTx(ctx, func(tx *sqlx.Tx) error {
		var count int64
		err := tx.GetContext(ctx, &count,
			"SELECT prefix_count FROM "+namespaceRegistryTable+" WHERE name = $1 FOR UPDATE",
			namespace,
		)
		if errors.Is(err, dbsql.ErrNoRows) {
			return ErrNamespaceDoesNotExist
		}
		if err != nil {
			if isUndefinedTable(err) {
				return ErrNamespaceDoesNotExist
			}
			return fmt.Errorf("unable to lock namespace row:%w", err)
		}
		if count > 0 {
			return errNamespaceHasPrefixes
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+namespaceRegistryTable+" WHERE name = $1", namespace); err != nil {
			return fmt.Errorf("unable to delete namespace directory row:%w", err)
		}
		if _, err := tx.ExecContext(ctx, "DROP TABLE "+table); err != nil {
			if isUndefinedTable(err) {
				return ErrNamespaceDoesNotExist
			}
			return fmt.Errorf("unable to delete namespace table:%w", err)
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrNamespaceDoesNotExist) {
			s.tables.Delete(namespace)
		}
		return err
	}
	s.tables.Delete(namespace)
	return nil
}
