package tcdb

func (c *CDBReader) move_to(rpos, klen uint32) error {
	n := cdb_unpack(c.mmap[rpos+4:])
	if c.dend < n || c.dend-n < rpos+8+klen {
		return SomethingIsWrongError
	}
	c.kpos = rpos + 8
	c.klen = klen
	c.vpos = c.kpos + klen
	c.vlen = n
	return nil
}

func (c *CDBReader) NextKey() error {
	rpos := c.vpos + c.vlen
	if rpos > c.dend-8 {
		return SomethingIsWrongError
	}

	klen := cdb_unpack(c.mmap[rpos:])
	if c.dend-klen < rpos+8 {
		return SomethingIsWrongError
	}

	return c.move_to(rpos, klen)
}

func (c *CDBReader) read_data(buf []byte) (int, error) {
	dl := int(c.vlen)
	if dl > len(buf) {
		return 0, BufferTooSmallError
	}
	return c.ReadAt(buf[:dl], int64(c.vpos))
}

func (c *CDBReader) read_key(buf []byte) (int, error) {
	dl := int(c.klen)
	if dl > len(buf) {
		return 0, BufferTooSmallError
	}
	return c.ReadAt(buf[:dl], int64(c.kpos))
}
