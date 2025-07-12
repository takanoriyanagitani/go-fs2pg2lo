package fs2pg2lo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"

	pgx "github.com/jackc/pgx/v5"
	pgxpool "github.com/jackc/pgx/v5/pgxpool"
)

const FileSizeThresholdDefault FileSizeThreshold = 1048576

const LimitReaderLimitDefault int64 = 16777216

const sqlTableExists = `
	SELECT 1
	FROM information_schema.tables
	WHERE table_schema = 'public' AND table_name = $1
`

const sqlCreateTable = `
	CREATE TABLE IF NOT EXISTS %s (
		id               BIGSERIAL PRIMARY KEY,
		filename         TEXT NOT NULL,
		size             BIGINT NOT NULL,
		small_content    BYTEA,
		large_content_id OID,
		CONSTRAINT %s_chk CHECK(
			1 = (
				(CASE small_content WHEN NULL THEN 0 ELSE 1 END)
				+ (CASE large_content_id WHEN NULL THEN 0 ELSE 1 END)
			)
		)
	)
`

var (
	ErrInvalidLimit error = errors.New("too small limit")
)

type TableCreator func(context.Context, TableName) error

type StandardDb struct{ *sql.DB }

func (s StandardDb) ToTableCreator() TableCreator {
	return func(ctx context.Context, tableName TableName) error {
		var query string = fmt.Sprintf(sqlCreateTable, tableName.checked, tableName.checked)
		_, err := s.ExecContext(ctx, query)
		return err
	}
}

func (s StandardDb) ToIsTableExists() IsTableExists {
	return func(ctx context.Context, unchecked UncheckedTableName) bool {
		var dummy int
		err := s.QueryRowContext(ctx, sqlTableExists, unchecked.unchecked).Scan(&dummy)
		return err == nil
	}
}

type PgxPool struct{ *pgxpool.Pool }

func (p PgxPool) ToIsTableExists() IsTableExists {
	return func(ctx context.Context, unchecked UncheckedTableName) bool {
		var dummy int
		err := p.QueryRow(ctx, sqlTableExists, unchecked.unchecked).Scan(&dummy)
		return err == nil
	}
}

func (p PgxPool) ToTableCreator() TableCreator {
	return func(ctx context.Context, tableName TableName) error {
		var query string = fmt.Sprintf(sqlCreateTable, tableName.checked, tableName.checked)
		_, err := p.Exec(ctx, query)
		return err
	}
}

func (p PgxPool) ToFileStoreDefault() FileStore {
	return FileStore{
		Pool:              p.Pool,
		FileSizeThreshold: FileSizeThresholdDefault,
	}
}

type PoolConfig struct{ *pgxpool.Config }

func (p PoolConfig) Connect(ctx context.Context) (PgxPool, error) {
	pool, err := pgxpool.NewWithConfig(ctx, p.Config)
	return PgxPool{Pool: pool}, err
}

type PoolConfigString string

const PoolConfigStringDefault PoolConfigString = ""

func (s PoolConfigString) Parse() (PoolConfig, error) {
	cfg, err := pgxpool.ParseConfig(string(s))
	return PoolConfig{Config: cfg}, err
}

type FileStore struct {
	Pool              *pgxpool.Pool
	FileSizeThreshold FileSizeThreshold
}

func (f FileStore) SaveSmallFile(
	ctx context.Context,
	table TableName,
	file SmallFile,
) error {
	var filename string = file.Fullname
	var size int64 = int64(len(file.Content))
	_, err := f.Pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (filename, size, small_content)
		VALUES ($1, $2, $3)
	`, table.checked), filename, size, file.Content)
	return err
}

func (f FileStore) SaveLargeObject(
	ctx context.Context,
	table TableName,
	file ReaderWithMeta,
) (err error) {
	tx, err := f.Pool.Begin(ctx)
	if err != nil {
		return err
	}

	err = f.saveLargeObject(ctx, table, file, tx)
	switch err {
	case nil:
		return tx.Commit(ctx)
	default:
		return errors.Join(err, tx.Rollback(ctx))
	}
}

func (f FileStore) ToFileSaver(
	tname TableName,
	limit int64,
) FileSaver {
	if limit < int64(f.FileSizeThreshold) {
		return func(_ context.Context, _ ReaderWithMeta) error {
			return fmt.Errorf("%w: %v", ErrInvalidLimit, limit)
		}
	}
	return func(ctx context.Context, rmeta ReaderWithMeta) error {
		if f.FileSizeThreshold.IsLarge(rmeta.FileInfo) {
			return f.SaveLargeObject(ctx, tname, rmeta)
		}

		smallFile, err := rmeta.ToSmallFile(limit)
		if err != nil {
			return err
		}

		return f.SaveSmallFile(ctx, tname, smallFile)
	}
}

func (f FileStore) ToFileSaverDefault() FileSaver {
	return f.ToFileSaver(TableName{checked: TABLE_NAME}, LimitReaderLimitDefault)
}

func (f FileStore) insertLargeObjectInfo(
	ctx context.Context,
	table TableName,
	filename string,
	size int64,
	oid uint32,
) (err error) {
	var query string = fmt.Sprintf(`
		INSERT INTO %s (filename, size, large_content_id)
		VALUES ($1, $2, $3)
	`, table.checked)
	_, err = f.Pool.Exec(ctx, query, filename, size, oid)
	return err
}

func (f FileStore) saveLargeObject(
	ctx context.Context,
	table TableName,
	file ReaderWithMeta,
	tx pgx.Tx,
) (err error) {
	var lobj pgx.LargeObjects = tx.LargeObjects()
	lo := LargeObjects{&lobj}
	var rdr io.Reader = file.Reader

	var filename string = file.ExtendedFileInfo.Fullname
	var size int64 = file.ExtendedFileInfo.FileInfo.Size()

	oid, err := lobj.Create(ctx, 0)
	if nil != err {
		return err
	}

	err = lo.AppendFromReader(
		ctx,
		oid,
		rdr,
	)
	if nil != err {
		return err
	}

	return f.insertLargeObjectInfo(ctx, table, filename, size, oid)
}
