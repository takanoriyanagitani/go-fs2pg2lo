package fs2pg2lo

import (
	"context"
	"errors"
	"io"

	pgx "github.com/jackc/pgx/v5"
)

const DB_NAME string = "go_fs2pg2lo"

const TABLE_NAME string = "go_fs2pg2lo_files"

type LargeObjects struct{ objs *pgx.LargeObjects }

// CopyToWriter opens the specified large object and copies its contents
// to the provided io.Writer.
//
// # Arguments
//
//   - ctx: The context for the operation.
//   - oid: The oid of the existing large object.
//   - wtr: The target writer.
func (l LargeObjects) CopyToWriter(
	ctx context.Context,
	oid uint32,
	wtr io.Writer,
) error {
	r, err := l.objs.Open(ctx, oid, pgx.LargeObjectModeRead)
	if err != nil {
		return err
	}
	_, err = io.Copy(wtr, r)
	return errors.Join(err, r.Close())
}

// Appends the content from the reader to the specified empty large object.
//
// # Arguments
//
//   - ctx: The context for the operation.
//   - oid: The oid of the existing empty large object.
//   - rdr: The source reader.
func (l LargeObjects) AppendFromReader(
	ctx context.Context,
	oid uint32,
	rdr io.Reader,
) error {
	w, err := l.objs.Open(ctx, oid, pgx.LargeObjectModeWrite)
	if err != nil {
		return err
	}
	_, err = io.Copy(w, rdr)
	return errors.Join(err, w.Close())
}

type FileSaver func(context.Context, ReaderWithMeta) error

func (s FileSaver) SaveNamedFile(ctx context.Context, named NamedFile) (err error) {
	rmeta, err := named.ToReaderWithMeta(named.Fullname)
	if err != nil {
		return err
	}

	err = s(ctx, rmeta)
	return errors.Join(err, named.Close())
}

func (s FileSaver) SaveFile(ctx context.Context, filename string) error {
	fname := Filename{filename}
	namedFile, err := fname.ToNamedFile()
	if err != nil {
		return err
	}
	return s.SaveNamedFile(ctx, namedFile)
}
