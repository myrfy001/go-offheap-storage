package offheapstorage

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkOffheapCore(b *testing.B){
	addrs := make([]uint64, 0, 1024*1024*10)
	data := [32768]byte{}
	dataRead := make([]byte, 32768)
	for i:=0; i<len(data); i++ {
		data[i] = byte(i % 256)
	}

	s := NewOffHeapStorageCore()

	

	for i := 0; i<10240; i++ {
		for j:=1; j<=1024; j++ {
			addr,err := s.Put(data[:j])
			if err != nil {
				panic(err)
			}
			addrs = append(addrs, addr)
		}
	}

	b.ResetTimer()

	for _, addr := range addrs{
		size, err := s.Get(addr, dataRead)
		if err != nil {
			panic(err)
		}
		if string(data[:size]) != string(dataRead[:size]) {
			panic("Data not match")
		}
		_ = addr
	} 

	time.Sleep(100*time.Second)
	

}

func BenchmarkMap(b *testing.B){
	addrs := make([]uint64, 0, 1024*1024*10)
	data := [32768]byte{}
	for i:=0; i<len(data); i++ {
		data[i] = byte(i % 256)
	}

	s := make(map[uint64][]byte)

	
	addr := uint64(0)
	for i := 0; i<10240; i++ {
		for j:=1; j<=1024; j++ {
			addr++
			tmp := make([]byte, j, j)
			copy(tmp, data[:j])
			s[addr] = tmp
			addrs = append(addrs, addr)
		}
	}

	b.ResetTimer()

	for _, addr := range addrs{
		dataRead, ok := s[addr]
		if ok != true {
			panic("")
		}
		if string(dataRead) != string(data[:len(dataRead)]) {
			fmt.Println("Data not match", data, dataRead[:len(data)])
			panic("Data Not Match")
		}
		_ = addr
	} 

	time.Sleep(10*time.Second)


}