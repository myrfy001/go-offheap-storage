package offheapstorage

import (
	"math/bits"
)

type bitMap struct {
    bits []uint64
}

func (b *bitMap) Set(idx uint) {
    slot := idx >> 6
    pos := idx % 64
    if int(slot)+1 > len(b.bits) {
        b.bits = append(b.bits, make([]uint64, int(slot)-len(b.bits)+1)...)
    }
    b.bits[slot] |= (1 << pos)
}

func (b *bitMap) UnSet(idx uint) {
    slot := idx >> 6
    pos := idx % 64
    if int(slot)+1 > len(b.bits) {
        b.bits = append(b.bits, make([]uint64, int(slot)-len(b.bits)+1)...)
    }
    b.bits[slot] &= (^uint64(1 << pos))
}

func (b *bitMap) Get(idx uint) bool {
    slot := idx >> 6
    pos := idx % 64
    if int(slot)+1 > len(b.bits) {
        return false
    }
    return (b.bits[slot]&uint64(1<<pos) != 0)
}

func (b *bitMap) GetFirstSetBit() int {
    i := 0
    for ; i < len(b.bits) && b.bits[i] == 0; i++ {
    }
    if i == len(b.bits) {
        return -1
    }
    return int(i*64 + bits.TrailingZeros64(b.bits[i]))
}

func (b *bitMap) GetFirstUnsetBit() int {
    i := 0
    for ; i < len(b.bits) && b.bits[i] == 0xffffffffffffffff; i++ {
    }
    if i == len(b.bits) {
        return i * 64
    }
    return int(i*64 + bits.TrailingZeros64(^b.bits[i]))
}