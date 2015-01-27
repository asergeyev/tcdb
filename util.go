package tcdb

type recptr struct {
	hval, rpos uint32
}

type table struct {
	next *table
	recs []*recptr
}

func cdb_hash(buf []byte) uint32 {
	hash := uint32(5381)
	for _, b := range buf {
		hash = (hash + (hash << 5)) ^ uint32(b)
	}
	return hash
}

func cdb_unpack(msg []byte) (n uint32) {
	return uint32(msg[3])<<24 | uint32(msg[2])<<16 | uint32(msg[1])<<8 | uint32(msg[0])
}

func cdb_pack(v uint32, buf []byte) []byte {
	buf[0] = byte(v)
	v >>= 8
	buf[1] = byte(v)
	v >>= 8
	buf[2] = byte(v)
	buf[3] = byte(v >> 8)
	return buf[:4]
}
