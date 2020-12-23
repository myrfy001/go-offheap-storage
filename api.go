package offheapstorage
type OffHeapStorage struct {
	core *OffHeapStorageCore
	bigDataSize map[uint64]uint64
}

const partSizeForLargeData = (32768 - 8)

func NewOffHeapStorage() *OffHeapStorage {
	ret := OffHeapStorage{
		core: NewOffHeapStorageCore(),
		bigDataSize: make(map[uint64]uint64),
	}
	return &ret
}

func (s *OffHeapStorage) Put(data []byte) (uint64, error) {
	dataTotalSize := uint64(len(data))
	if dataTotalSize <= 32768 {
		return s.core.Put(data)
	}
	leftSizeToSave := dataTotalSize
	partStartPos := leftSizeToSave - partSizeForLargeData
	savedAddrs := make([]uint64,0,dataTotalSize/partSizeForLargeData+1)
	nextAddr := uint64(0)
	var err error
	for partStartPos >= 0 {
		nextAddr, err = s.core.PutBig(data[partStartPos: leftSizeToSave], nextAddr)
		if err != nil {
			// if we have an error, we should rollback the stored items
			for _, addr := range savedAddrs {
				s.core.Delete(addr)
			}
			return 0, err
		}
		savedAddrs = append(savedAddrs, nextAddr)
		if partStartPos == 0 {
			// the last piece of data just fit in the part
			break
		}
		
		if partStartPos <= partSizeForLargeData {
			partStartPos = 0
		} else {
			partStartPos -= partSizeForLargeData
		}
		leftSizeToSave -= partSizeForLargeData
	}
	s.bigDataSize[nextAddr] = dataTotalSize
	return nextAddr, nil
}

func (s *OffHeapStorage) Get(addr uint64) ([]byte, error) {
	buf := make([]byte,32768)
	if addr & fullDataBitMask == 0{
		size, err := s.core.Get(addr,buf)
		if err != nil {
			return nil, err
		}
		return buf[:size], nil
	}
	dataTotalSize, ok := s.bigDataSize[addr]
	if !ok{
		return nil, ErrAddrNotExist
	}

	retBuf := make([]byte, dataTotalSize)
	readSize := 0
	var partData []byte
	var err error
	for {
		partData, addr, err = s.core.GetBig(addr)
		if err != nil {
			return nil, err
		}
		
		copy(retBuf[readSize:], partData)
		readSize += len(partData)
		if addr == 0 {
			break
		}
	}
	return retBuf, nil
}

func (s *OffHeapStorage) Delete(addr uint64) error {
	if addr & fullDataBitMask == 0{
		return s.core.Delete(addr)
	}
	var err error
	for {
		addr, err = s.core.DeleteBig(addr)
		if err != nil {
			return err
		}
		if addr == 0 {
			break
		}
	}
	return nil
}