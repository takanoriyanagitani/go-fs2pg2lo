package fs2pg2lo

import (
	"io"

	pgx "github.com/jackc/pgx/v5"
)

type LargeObjectReader struct{ rdr *pgx.LargeObject }

func (r LargeObjectReader) Close() error               { return r.rdr.Close() }
func (r LargeObjectReader) Read(p []byte) (int, error) { return r.rdr.Read(p) }

func (r LargeObjectReader) Seek(offset int64, whence int) (int64, error) {
	return r.rdr.Seek(offset, whence)
}

func (r LargeObjectReader) AsReadCloser() io.ReadCloser         { return r }
func (r LargeObjectReader) AsReadSeekCloser() io.ReadSeekCloser { return r }

type LargeObjectWriter struct{ wtr *pgx.LargeObject }

func (w LargeObjectWriter) Close() error                  { return w.wtr.Close() }
func (w LargeObjectWriter) Write(p []byte) (int, error)   { return w.wtr.Write(p) }
func (w LargeObjectWriter) AsWriteCloser() io.WriteCloser { return w }
