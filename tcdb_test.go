package tcdb

import (
	"fmt"
	"os"
	"testing"
)

func TestHash(t *testing.T) {
	if cdb_hash([]byte{}) != 5381 {
		t.Error("Invalid hash implementaion")
	}
}

func TestCreate(t *testing.T) {
	tmp := fmt.Sprintf(os.TempDir()+"/cdb-%d.cdb", os.Getpid())
	//defer os.Remove(tmp)

	f, err := Create(tmp)
	if err != nil {
		t.Error(err)
	}
	f.Add([]byte("str"), []byte("val"))
	fmt.Printf("Worked: %+v\n", f)
	f.Close()
}
