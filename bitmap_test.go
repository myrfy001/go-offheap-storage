package offheapstorage

import (
	"testing"
)

func TestBitMap(t *testing.T){
	b := &bitMap{}

	if b.Get(100000) == true{
		t.Errorf("Err 1")
		t.Fail()
	}

	if  b.GetFirstSetBit() != -1 {
		t.Errorf("Err 2")
		t.Fail()
	}

	b.Set(100000)

	if b.Get(100000) == false{
		t.Errorf("Err 3")
		t.Fail()
	}

	if  b.GetFirstSetBit() != 100000 {
		t.Errorf("Err 4")
		t.Fail()
	}

	b.UnSet(100000)

	if b.Get(100000) == true{
		t.Errorf("Err 5")
		t.Fail()
	}

	if  b.GetFirstSetBit() != -1 {
		t.Errorf("Err 6")
		t.Fail()
	}

	b.Set(0)

	if b.Get(0) == false{
		t.Errorf("Err 7")
		t.Fail()
	}

	if  b.GetFirstSetBit() != 0 {
		t.Errorf("Err 8")
		t.Fail()
	}

	b.UnSet(0)

	if b.Get(0) == true{
		t.Errorf("Err 9")
		t.Fail()
	}

	if  b.GetFirstSetBit() != -1 {
		t.Errorf("Err 10")
		t.Fail()
	}

	
	if  b.GetFirstUnsetBit() != 0 {
		t.Errorf("Err 11")
		t.Fail()
	}

	b.Set(0)
	if  b.GetFirstUnsetBit() != 1 {
		t.Errorf("Err 12")
		t.Fail()
	}

	b.Set(1)
	if  b.GetFirstUnsetBit() != 2 {
		t.Errorf("Err 13")
		t.Fail()
	}

	// 新建一个
	b = &bitMap{}
	if  b.GetFirstUnsetBit() != 0 {
		t.Errorf("Err 14")
		t.Fail()
	}

	for i:=0; i<128; i++ {
		b.Set(uint(i))
		if b.GetFirstUnsetBit() != i+1 {
			t.Errorf("Err 15")
		t.Fail()
		}
	}

}