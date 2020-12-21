package offheapstorage

import "fmt"

const (
	smallSizeDiv     = 8
	smallSizeMax     = 1024
	largeSizeDiv     = 128
	blockSize        = 4 * 1024 * 1024
	spanSizeClassCnt = 67
	maxSpanSize      = 32768

	blockIdxBitOffset      = 19
	spanSizeClassBitOffset = 39

	blockInnerAddrBitsMask = 0x7ffff
	blockIdxBitsMask       = 0xfffff
	spanSizeClasBitsMask   = 0x7f
)

var (
	ErrDataTooBig   = fmt.Errorf("Data Too Big")
	ErrAddrNotExist = fmt.Errorf("Addr Not Exist")
)

var classToSize = [spanSizeClassCnt]uint16{0, 8, 16, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240, 256, 288, 320, 352, 384, 416, 448, 480, 512, 576, 640, 704, 768, 896, 1024, 1152, 1280, 1408, 1536, 1792, 2048, 2304, 2688, 3072, 3200, 3456, 4096, 4864, 5376, 6144, 6528, 6784, 6912, 8192, 9472, 9728, 10240, 10880, 12288, 13568, 14336, 16384, 18432, 19072, 20480, 21760, 24576, 27264, 28672, 32768}

var sizeToClassSmall = [smallSizeMax/smallSizeDiv+1]int{}
var sizeToClassLarge = [(maxSpanSize-smallSizeMax)/largeSizeDiv+1]int{}

func init() {
	generateSize2ClassMap()
}

// divRoundUp returns ceil(n / a).
func divRoundUp(n, a int) int {
	// a is generally a power of two. This will get inlined and
	// the compiler will optimize the division.
	return (n + a - 1) / a
}

func generateSize2ClassMap() {

	for i := range sizeToClassSmall {
		size := i*smallSizeDiv
		for j, c := range classToSize {
			if int(c) >= size {
				sizeToClassSmall[i] = j
				break
			}
		}
	}

	for i := range sizeToClassLarge {
		size := smallSizeMax + i*largeSizeDiv
		for j, c := range classToSize {
			if int(c) >= size {
				sizeToClassLarge[i] = j
				break
			}
		}
	}
}

/*
	For address to be stored in js or lua, which only suppory double, we only use lower 52 bits
	19 bits for Addr in 4M block, since all address aligns to 8byte, wen only use 19 bits to address 4MByte space
	7  bits for span size class
	1  bit  for indicate whether all data fits in this span size, so we can store data larger than the max span size into multi cell
	20 bits for block index in a span size class
	5  bits for reserve use

	+----------+----------+-----------------+------------------------------+-----------------+
	|   51     |  50 - 46 |    45 - 39      |           38 - 19            |     18 - 0      |
	+----------+----------+-----------------+------------------------------+-----------------+
	| FullData | Reserved | Span Size Class | Block Idx In Span Size Class | Offset In Block |
	+----------+----------+-----------------+------------------------------+-----------------+
*/

type OffHeapStorageCore struct {
	spans        [spanSizeClassCnt]*memSpan
	addr2SizeMap map[uint64]uint16
}

type memSpan struct {
	blocks   []*memBlock
	usageMap bitMap
	cellSize int32
}

type memBlock struct {
	buf         [blockSize]byte
	usageMap    bitMap
	cellSize    int32
	cellCnt     int32
	usedCellCnt int32
}

func NewOffHeapStorageCore() *OffHeapStorageCore {
	ret := OffHeapStorageCore{
		addr2SizeMap: make(map[uint64]uint16),
	}
	for i := 0; i < len(ret.spans); i++ {
		ret.spans[i] = newMemSpan(int32(classToSize[i]))
	}
	return &ret
}

func (s *OffHeapStorageCore) Put(data []byte) (uint64, error) {
	dataSize := len(data)
	spanClassIdx := 0
	if dataSize <= smallSizeMax-8 {
		spanClassIdx = sizeToClassSmall[divRoundUp(dataSize, smallSizeDiv)]
	} else if dataSize <= maxSpanSize {
		spanClassIdx = sizeToClassLarge[divRoundUp(dataSize-smallSizeMax, largeSizeDiv)]
	} else {
		return 0, ErrDataTooBig
	}

	span := s.spans[spanClassIdx]

	addr := span.Put(data)
	addr |= uint64(spanClassIdx << spanSizeClassBitOffset)
	s.addr2SizeMap[addr] = uint16(dataSize)
	return addr, nil
}

func (s *OffHeapStorageCore) Get(addr uint64, buf []byte) (uint16, error) {
	size, ok := s.addr2SizeMap[addr]
	if !ok {
		return 0, ErrAddrNotExist
	}

	spanClassIdx := (addr >> spanSizeClassBitOffset) & spanSizeClasBitsMask
	if spanClassIdx >= uint64(len(s.spans)) {
		return 0, ErrAddrNotExist
	}
	span := s.spans[spanClassIdx]
	return size, span.Get(addr, size, buf)
}

func (s *OffHeapStorageCore) Delete(addr uint64) error {
	if _, ok := s.addr2SizeMap[addr]; !ok {
		return ErrAddrNotExist
	}
	delete(s.addr2SizeMap, addr)

	spanClassIdx := (addr >> spanSizeClassBitOffset) & spanSizeClasBitsMask
	if spanClassIdx >= uint64(len(s.spans)) {
		return ErrAddrNotExist
	}
	span := s.spans[spanClassIdx]
	return span.Delete(addr)
}

func newMemSpan(cellSize int32) *memSpan {
	return &memSpan{
		cellSize: cellSize,
	}
}

func (s *memSpan) Put(data []byte) uint64 {
	freeBlockIdx := s.usageMap.GetFirstUnsetBit()
	if freeBlockIdx == len(s.blocks) {
		newBlock := newMemBlock(s.cellSize)
		s.blocks = append(s.blocks, newBlock)
	}
	freeBlock := s.blocks[freeBlockIdx]
	addr := freeBlock.Put(data)

	if freeBlock.cellCnt == freeBlock.usedCellCnt {
		s.usageMap.Set(uint(freeBlockIdx))
	}

	addr |= uint64(freeBlockIdx << blockIdxBitOffset)
	return addr
}

func (s *memSpan) Get(addr uint64, size uint16, buf []byte) error {
	blockIdx := (addr >> blockIdxBitOffset) & blockIdxBitsMask
	if blockIdx >= uint64(len(s.blocks)) {
		return ErrAddrNotExist
	}
	block := s.blocks[blockIdx]
	return block.Get(addr, size, buf)

}

func (s *memSpan) Delete(addr uint64) error {
	blockIdx := (addr >> blockIdxBitOffset) & blockIdxBitsMask
	if blockIdx >= uint64(len(s.blocks)) {
		return ErrAddrNotExist
	}
	block := s.blocks[blockIdx]

	if err := block.Delete(addr); err != nil {
		return err
	}

	if block.cellSize != block.usedCellCnt {
		s.usageMap.UnSet(uint(blockIdx))
	}
	return nil
}

func newMemBlock(cellSize int32) *memBlock {
	return &memBlock{
		cellCnt:  int32(blockSize / cellSize),
		cellSize: cellSize,
	}
}

func (b *memBlock) Put(data []byte) uint64 {

	freeCellIdx := b.usageMap.GetFirstUnsetBit()
	b.usageMap.Set(uint(freeCellIdx))
	b.usedCellCnt++
	startOffset := int(freeCellIdx) * int(b.cellSize)
	copy(b.buf[startOffset:], data)
	return uint64(startOffset) >> 3
}

func (b *memBlock) Get(addr uint64, size uint16, buf []byte) error {
	addrInCell := uint64(addr&blockInnerAddrBitsMask) << 3
	cellIdx := addrInCell / uint64(b.cellSize)
	if cellIdx >= uint64(b.cellCnt) {
		return ErrAddrNotExist
	}
	copy(buf, b.buf[addrInCell:(addrInCell+uint64(size))])
	return nil
}

func (b *memBlock) Delete(addr uint64) error {
	cellIdx := (uint64(addr&blockInnerAddrBitsMask) << 3) / uint64(b.cellSize)
	if cellIdx >= uint64(b.cellCnt) {
		return ErrAddrNotExist
	}
	b.usageMap.UnSet(uint(cellIdx))
	b.usedCellCnt--
	return nil
}
