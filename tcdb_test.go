package tcdb

import (
	"fmt"
	"os"
	"testing"
)

func TestHash(t *testing.T) {
	if classic_cdb_hash([]byte{}) != 5381 {
		t.Error("Invalid hash implementaion")
	}
}

func TestCreateFind(t *testing.T) {
	tmp := fmt.Sprintf(os.TempDir()+"/cdb-%d.cdb", os.Getpid())
	defer os.Remove(tmp)

	f, err := Create(tmp)
	if err != nil {
		t.Fatal(err)
	}
	f.Add([]byte("str"), []byte("val"))
	if f.Close() != nil {
		t.Error("Could not close file after writing")
	}

	fr, err := Open(tmp)
	if err != nil {
		t.Fatal(err)
	}

	// Reading without lookup should get you right key
	key, err := fr.GetKey()
	if err != nil {
		t.Fatal(err)
	}
	if string(key) != "str" {
		t.Error("Invalid key read back after single key cdb created")
	}
	val, err := fr.GetData()
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "val" {
		t.Error("Invalid value read back after single key cdb created")
	}
	if fr.NextKey() == nil {
		t.Error("Next key shold return error")
	}
	res, err := fr.Find(key)
	if err != nil {
		t.Error(err)
	}
	if !res {
		t.Error("Find should return true")
	}
	if fr.kpos != 2048+8 {
		t.Error(fr.kpos)
	}
	if fr.Close() != nil {
		t.Error("Could not close file after reading")
	}
}
