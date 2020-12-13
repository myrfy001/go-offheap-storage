package offheapstorage

import (
	"testing"
)

func TestOffheapCore(t *testing.T){
	
	data := [32768]byte{}
	dataRead := make([]byte, 32768)
	for i:=0; i<len(data); i++ {
		data[i] = byte(i % 256)
	}

	b := NewOffHeapStorageCore()

	for i := 0; i<8; i++ {
		for j:=1; j<=32768; j++ {
			addr,err := b.Put(data[:j])
			if err != nil {
				panic(err)
			}
			size, err := b.Get(addr, dataRead)
			if err != nil {
				t.Log("i=", i, "j=", j, "addr=", addr)
				panic(err)
			}
			if string(data[:j]) != string(dataRead[:size]) {
				panic("Data not match")
			}
		}
	}

	

}