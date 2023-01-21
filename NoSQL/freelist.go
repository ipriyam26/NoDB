package main

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