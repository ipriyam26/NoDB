package main

import "encoding/binary"

// const pageNumSize = 8

type freeList struct{
	maxPage pageNum;
	releasedPages []pageNum;
}

func newFreeList() *freeList  {
	return &freeList{
		maxPage: 0,
		releasedPages: []pageNum{},
	}
	
}
func (fr *freeList) getNextPage() pageNum  {
	if len(fr.releasedPages ) !=0{
		pageId := fr.releasedPages[len(fr.releasedPages)-1]
		fr.releasedPages = fr.releasedPages[:len(fr.releasedPages)-1]
		return pageId
	}
	
	fr.maxPage+=1
	return fr.maxPage
}

func (fr *freeList) releasePage(page pageNum)  {
	fr.releasedPages = append(fr.releasedPages, page)

}



func (fr *freeList) serialize(buf []byte)  []byte{

	pos:=0

	binary.LittleEndian.PutUint16(buf[pos:], uint16(fr.maxPage))

	pos+=2

	binary.LittleEndian.PutUint16(buf[pos:],uint16(len(fr.releasedPages)))

	pos+=2
	for _, page := range fr.releasedPages {
		binary.LittleEndian.PutUint64(buf[pos:], uint64(page))
		pos += pageNumSize

	}
	return buf

}

func (fr *freeList) deserialize(buf []byte) {
	pos:=0
	fr.maxPage = pageNum(binary.LittleEndian.Uint16(buf[pos:]))
	pos+=2
	releasedPageCount := int(binary.LittleEndian.Uint16(buf[pos:]))
	pos+=2
	for i := 0; i < releasedPageCount; i++ {
		fr.releasedPages = append(fr.releasedPages, pageNum(binary.LittleEndian.Uint16(buf[pos:])))
		pos+=pageNumSize
	}
}