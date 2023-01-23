package main

import "encoding/binary"
const metaPageNum = 0

// stored at first 0 pageNumber contains the page number of freeListPage in our case
type meta struct{
    freeListPage pageNum;
}

func newEmptyMeta() *meta {
    return &meta{}
}

// Convert freeLisPage to bytes so that they can be written to memory
func (m *meta) serialize(buf []byte) {
    pos :=0

    binary.LittleEndian.PutUint64(
        buf[pos:],
        uint64(
            m.freeListPage,
        ),
    )

    pos+=pageNumSize
    
}

// Convert the bytes from memory to freeListPage
func (m *meta) deserialize(buf []byte) {
    pos := 0
    m.freeListPage = pageNum(binary.LittleEndian.Uint64(buf[pos:]))
    pos+=pageNumSize
}