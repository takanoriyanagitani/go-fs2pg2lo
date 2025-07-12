package fs2pg2lo

import (
	"fmt"
	"io"
	"io/fs"
	"os"
)

type ExtendedFileInfo struct {
	fs.FileInfo

	Fullname string
}

type ReaderWithMeta struct {
	io.Reader

	ExtendedFileInfo
}

func (r ReaderWithMeta) CheckLimit(limit int64) error {
	var size int64 = r.ExtendedFileInfo.FileInfo.Size()
	var tooSmallLimit bool = limit < size
	switch tooSmallLimit {
	case true:
		return fmt.Errorf("%w: %v", ErrInvalidLimit, limit)
	default:
		return nil
	}
}

func (r ReaderWithMeta) ToSmallFile(limit int64) (SmallFile, error) {
	err := r.CheckLimit(limit)
	if err != nil {
		return SmallFile{}, err
	}
	limitedReader := io.LimitReader(r.Reader, limit)
	content, err := io.ReadAll(limitedReader)
	return SmallFile{
		Fullname: r.Fullname,
		Content:  content,
	}, err
}

type FileSize int64

type FileSizeThreshold int64

func (t FileSizeThreshold) IsSmallFile(s FileSize) bool {
	var fileSize int64 = int64(s)
	return fileSize <= int64(t)
}

func (t FileSizeThreshold) IsSmall(s fs.FileInfo) bool {
	return t.IsSmallFile(FileSize(s.Size()))
}

func (t FileSizeThreshold) IsLarge(s fs.FileInfo) bool {
	return !t.IsSmall(s)
}

type SmallFile struct {
	Fullname string
	Content  []byte
}

type File struct{ fs.File }

func (f File) Close() error               { return f.File.Close() }
func (f File) Stat() (fs.FileInfo, error) { return f.File.Stat() }
func (f File) Read(p []byte) (int, error) { return f.File.Read(p) }
func (f File) AsReader() io.Reader        { return f }

func (f File) ToExtendedFileInfo(name string) (ExtendedFileInfo, error) {
	info, err := f.Stat()
	return ExtendedFileInfo{
		FileInfo: info,
		Fullname: name,
	}, err
}

func (f File) ToReaderWithMeta(name string) (ReaderWithMeta, error) {
	extendedInfo, err := f.ToExtendedFileInfo(name)
	return ReaderWithMeta{
		Reader:           f,
		ExtendedFileInfo: extendedInfo,
	}, err
}

type NamedFile struct {
	File

	Fullname string
}

func (n NamedFile) Close() error { return n.File.Close() }

type Filename struct{ Fullname string }

func (f Filename) ToFile() (File, error) {
	file, err := os.Open(f.Fullname)
	return File{File: file}, err
}

func (f Filename) ToNamedFile() (NamedFile, error) {
	file, err := f.ToFile()
	return NamedFile{
		Fullname: f.Fullname,
		File:     file,
	}, err
}
