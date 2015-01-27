// tcdb - primitive frivolous go translation of tinycdb lib.
package tcdb

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"launchpad.net/gommap"
	"os"
)

var BufferTooSmallError = errors.New("Not enough space in buffer")
var TooManyRecordsError = errors.New("Could not save all records")

type CDBReader struct {
	f     *os.File
	fsize uint32
	dend  uint32
	m     gommap.MMap

	vpos, vlen uint32
	kpos, klen uint32
}

func (c *CDBReader) DPos() uint32 { return c.vpos }
func (c *CDBReader) DLen() uint32 { return c.vlen }
func (c *CDBReader) KPos() uint32 { return c.kpos }
func (c *CDBReader) KLen() uint32 { return c.klen }

func Open(fname string) (*CDBReader, error) {
	return nil, nil
}

func (c *CDBReader) Close() error {
	return nil
}

func (c *CDBReader) ReadAt(buf []byte, off int64) (int, error) {
	return 0, nil
}

// Read is an equivalent to ReadData
func (c *CDBReader) Read(buf []byte) (int, error) {
	return c.ReadData(buf)
}

func (c *CDBReader) ReadData(buf []byte) (int, error) {
	dl := int(c.DLen())
	if dl > len(buf) {
		return 0, BufferTooSmallError
	}
	return c.ReadAt(buf[:dl], int64(c.DPos()))
}

func (c *CDBReader) ReadKey(buf []byte) (int, error) {
	dl := int(c.KLen())
	if dl > len(buf) {
		return 0, BufferTooSmallError
	}
	return c.ReadAt(buf[:dl], int64(c.KPos()))
}

func (c *CDBReader) GetData() ([]byte, error) {
	return nil, nil
}

func (c *CDBReader) GetKey() ([]byte, error) {
	return nil, nil
}

func (c *CDBReader) Find(key []byte) ([][]byte, error) {
	return nil, nil
}

type key_iterator struct {
	*CDBReader
	hval             uint32
	htp, htab, htend []byte
	httodo           uint32
	key              []byte
}

func (c *CDBReader) FindStart(key []byte) (*key_iterator, error) {
	return nil, nil
}

func (c *key_iterator) FindNext() error {
	return nil
}

// #define cdb_seqinit(cptr, cdbp) ((*(cptr))=2048)
// int cdb_seqnext(unsigned *cptr, struct cdb *cdbp);
type CDBPutMode byte

const (
	PUT_ADD = CDBPutMode(iota)
	PUT_REPLACE
	PUT_INSERT
	PUT_WARN
)

const write_bufsize = 10240

type ioSWC interface {
	io.Writer
	io.Seeker
	io.Closer
}

type CDBWriter struct {
	file ioSWC
	b    *bufio.Writer
	dpos uint32
	rcnt uint32
	tabs [256]*table
}

func Create(fn string) (*CDBWriter, error) {
	f, err := os.Create(fn)
	if err != nil {
		return nil, fmt.Errorf("Could not create file: %s", err)
	}

	buf := bufio.NewWriterSize(f, write_bufsize)
	buf.Write(make([]byte, 2048))
	return &CDBWriter{file: f, b: buf, dpos: 2048}, nil
}

func (w *CDBWriter) Add(k, v []byte) error {
	w.add(cdb_hash(k), k, v)
	return nil
}

func (w *CDBWriter) Get(k []byte) bool {
	return false
}

func (w *CDBWriter) Put(k, v []byte, mode CDBPutMode) error {
	return nil
}

func (w *CDBWriter) Close() error {
	w.b.Flush()
	if err := w.finish(); err != nil {
		_ = w.file.Close()
		return err
	}
	return w.file.Close()
}
