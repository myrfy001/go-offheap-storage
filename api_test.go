package offheapstorage

import (
	"bytes"
	"testing"
)

func BenchmarkOffheapAPI(b *testing.B){
	// addrs := make([]uint64, 0, 1024)
	data1 := make([]byte, 32767)
	for i:=0; i<len(data1); i++ {
		data1[i] = byte(i % 256)
	}

	data2 := make([]byte, 32768)
	for i:=0; i<len(data2); i++ {
		data2[i] = byte(i % 256)
	}

	data3 := make([]byte, 32769)
	for i:=0; i<len(data3); i++ {
		data3[i] = byte(i % 256)
	}

	data4 := make([]byte, 123456)
	for i:=0; i<len(data4); i++ {
		data4[i] = byte(i % 256)
	}


	s := NewOffHeapStorage()

	b.ResetTimer()

	for x:=0; x<b.N; x++ {
		addr1,_ := s.Put(data1)
		addr2,_ := s.Put(data2)
		addr3,_ := s.Put(data3)
		addr4,_ := s.Put(data4)
	
		read1, _ := s.Get(addr1)
		read2, _ := s.Get(addr2)
		read3, _ := s.Get(addr3)
		read4, _ := s.Get(addr4)
		
		if !bytes.Equal(data1, read1) {
			panic("Data1 not match")
		}
		if !bytes.Equal(data2, read2) {
			panic("Data2 not match")
		}
		if !bytes.Equal(data3, read3) {
			panic("Data3 not match")
		}
		if !bytes.Equal(data4, read4) {
			panic("Data4 not match")
		}
	
		s.Delete(addr1)
		s.Delete(addr2)
		s.Delete(addr3)
		s.Delete(addr4)
	}
	

	

	



}
