// tcdb - primitive frivolous go translation of tinycdb lib.
package tcdb

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"launchpad.net/gommap"
	"os"
)

var BufferTooSmallError = errors.New("Not enough space in buffer")
var TooManyRecordsError = errors.New("Could not save all records")
var SomethingIsWrongError = errors.New("Issue reading CDB (a.k.a. protocol error)")

var Hashfunc = classic_cdb_hash

type CDBReader struct {
	ioR
	fsize uint32
	dend  uint32
	mmap  gommap.MMap // single region map, no pre-reading at this point

	vpos, vlen uint32
	kpos, klen uint32
}

func Open(fn string) (*CDBReader, error) {
	finf, err := os.Stat(fn)
	if err != nil {
		return nil, fmt.Errorf("Can't find file size: %s", err)
	}

	f, err := os.Open(fn)
	if err != nil {
		return nil, fmt.Errorf("Could not open: %s", err)
	}

	sz := finf.Size()
	if sz < 2048 || sz > 0xffffffff {
		return nil, fmt.Errorf("Invalid file size: %d", sz)
	}

	mm, err := gommap.Map(f.Fd(), gommap.PROT_READ, gommap.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("Could not mmap file: %s", err)
	}
	cdb := &CDBReader{ioR: f, mmap: mm, fsize: uint32(sz), dend: cdb_unpack(mm), vpos: 2048}
	// Jump to first key (since current vpos=2048 and vlen=0)
	cdb.NextKey()
	return cdb, nil
}

func (c *CDBReader) Close() error {
	_ = c.mmap.UnsafeUnmap()
	c.mmap = nil
	c.fsize = 0
	c.dend = 0
	return c.ioR.Close()
}

func (c *CDBReader) ReadAt(buf []byte, off int64) (int, error) {
	if off > int64(c.fsize) || int64(c.fsize)-off < int64(len(buf)) {
		return 0, SomethingIsWrongError
	}
	return copy(buf, c.mmap[off:]), nil
}

// Read just gets keys, not values, moves pointer to next key
func (c *CDBReader) Read(buf []byte) (int, error) {
	defer c.NextKey()
	return c.read_key(buf)
}

func (c *CDBReader) GetData() ([]byte, error) {
	buf := make([]byte, c.klen)
	n, err := c.read_data(buf)
	if err != nil {
		return nil, err
	}
	if uint32(n) != c.klen {
		return buf, SomethingIsWrongError
	}
	return buf, nil
}

func (c *CDBReader) GetKey() ([]byte, error) {
	buf := make([]byte, c.klen)
	n, err := c.read_key(buf)
	if err != nil {
		return nil, err
	}
	if uint32(n) != c.klen {
		return buf, SomethingIsWrongError
	}
	return buf, nil
}

func (c *CDBReader) Find(key []byte) (bool, error) {
	if uint32(len(key)) > c.dend {
		return false, SomethingIsWrongError
	}
	hval := Hashfunc(key)
	tab := c.mmap[(hval<<3)&2047:]
	cnt := cdb_unpack(tab[4:])
	if cnt == 0 {
		return false, nil
	}
	todo := cnt << 3
	pos := cdb_unpack(tab)
	if (cnt > (c.fsize >> 3)) ||
		(pos < c.dend) ||
		(pos > c.fsize) ||
		(todo > c.fsize-pos) {
		return false, SomethingIsWrongError
	}

	htab := c.mmap[pos:]
	klen := uint32(len(key))
	// start at probable position and go towards end
	for htp := htab[((hval>>8)%cnt)<<3 : todo]; len(htp) > 0; htp = htp[8:] {
		// get record position (klen,vlen,key,value)
		rpos := cdb_unpack(htp[4:])
		if rpos == 0 {
			return false, nil
		}
		if rhash := cdb_unpack(htp); rhash == hval {
			if rpos > c.dend-8 {
				return false, SomethingIsWrongError
			}
			if cdb_unpack(c.mmap[rpos:]) == klen {
				// our key?!
				if c.dend-klen < rpos+8 {
					return false, SomethingIsWrongError
				}
				if bytes.Equal(key, c.mmap[rpos+8:rpos+8+klen]) {
					// Yey! found it!
					return true, c.move_to(rpos, klen)
				}
			}
		}
	}
	return false, nil
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

type CDBWriter struct {
	ioW
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
	return &CDBWriter{ioW: f, b: buf, dpos: 2048}, nil
}

func (w *CDBWriter) Add(k, v []byte) error {
	return w.add(Hashfunc(k), k, v)
}

func (w *CDBWriter) Has(k []byte) bool {
	panic("Has is not implemented yet")
	return false
}

func (w *CDBWriter) Put(k, v []byte, mode CDBPutMode) error {
	panic("Put is not implemented yet")
	return nil
}

func (w *CDBWriter) Close() error {
	w.b.Flush()
	if err := w.finish(); err != nil {
		_ = w.ioW.Close()
		return err
	}
	return w.ioW.Close()
}
