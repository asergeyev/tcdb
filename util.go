package tcdb

func cdb_hash(buf []byte) uint32 {
	hash := uint32(5381)
	for _, b := range buf {
		hash = (hash + (hash << 5)) ^ uint32(b)
	}
	return 0
}

func cdb_unpack(msg []byte) uint32 {
	return uint32(msg[0])<<24 | uint32(msg[1])<<16 | uint32(msg[2])<<8 | uint32(msg[3])
}

func cdb_pack(v uint32, buf []byte) []byte {
	buf[0], buf[1], buf[2], buf[3] = byte(v>>24), byte(v>>16), byte(v>>8), byte(v)
	return buf[:4]
}
