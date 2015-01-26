package tcdb

type recptr struct {
	hval, rpos uint32
}

type table struct {
	next *table
	cnt  uint32
	recs [256]recptr
}
