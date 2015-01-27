package tcdb

import (
	"fmt"
)

func (w *CDBWriter) finish() error {
	if ((0xffffffff - w.dpos) >> 3) < w.rcnt {
		return TooManyRecordsError
	}
	// count htab sizes and reorder reclists
	hsize := uint32(0)
	hcnt := [256]uint32{}
	for tidx, rl := range w.tabs {
		var rlt *table
		cnt := uint32(0)
		for rl != nil {
			rln := rl.next
			rl.next, rlt = rlt, rl // order backwards
			cnt += uint32(len(rl.recs))
			rl = rln
		}
		w.tabs[tidx] = rlt
		hcnt[tidx] = cnt << 1
		if hsize < hcnt[tidx] {
			hsize = hcnt[tidx] // max size * 256
		}
	}

	// it's pretty in C when you can use same buffer but somewhat ugly in go for now
	htab := make([]recptr, hsize+2)
	htab = htab[2:]

	hpos := [256]uint32{}
	for tidx, rl := range w.tabs {
		hpos[tidx] = w.dpos
		if lnall := hcnt[tidx]; lnall != 0 {

			// for now I'd get second buffer
			p := make([]byte, lnall<<3)

			for ; rl != nil; rl = rl.next {
				for _, r := range rl.recs {
					hi := (r.hval >> 8) % lnall // start in this section
					for htab[hi].hval != 0 {    // iterate until found spot
						if hi++; hi == lnall {
							hi = 0
						}
					}
					htab[hi].hval = r.hval
					htab[hi].rpos = r.rpos
				}
			}
			for i, x := range htab {
				cdb_pack(x.hval, p[i<<3:])
				cdb_pack(x.rpos, p[(i<<3)+4:])
			}
			n, err := w.b.Write(p)
			if err != nil {
				return err
			}
			w.dpos += uint32(n)
		}
	}
	w.b.Flush()
	p := make([]byte, 2048)
	for i := range hpos {
		fmt.Println(hcnt[i])
		cdb_pack(hpos[i], p[i<<3:])
		cdb_pack(hcnt[i], p[(i<<3)+4:])
	}
	_, err := w.file.Seek(0, 0)
	if err != nil {
		return err
	}
	w.file.Write(p) // unbuffered here

	return nil
}

func (w *CDBWriter) add(hval uint32, k, v []byte) error {
	klen, vlen := uint32(len(k)), uint32(len(v))
	if klen > 0xffffffff-(w.dpos+8) || vlen > 0xffffffff-(w.dpos+klen+8) {
		return TooManyRecordsError
	}
	i := hval & 255
	t := w.tabs[i]
	if t == nil || len(t.recs) == cap(t.recs) {
		t = &table{recs: make([]*recptr, 0, 256), next: w.tabs[i]}
		w.tabs[i] = t
	}
	t.recs = append(t.recs, &recptr{hval, w.dpos})
	w.rcnt++
	out := make([]byte, 8)
	cdb_pack(klen, out)
	cdb_pack(vlen, out[4:])
	for _, b := range [][]byte{out, k, v} {
		wrlen, err := w.b.Write(b)
		if err != nil {
			return err
		}
		w.dpos += uint32(wrlen)
	}
	return nil
}
