package main

import (
	"errors"
	"fmt"
	"os"
)

type pageNum uint64

type page struct{
    num pageNum;
    data []byte;
}

// Data Access Layer
type dal struct{
    file *os.File;
    pageSize int;
    * freeList;
    * meta;
}


// create a new DAL 
func newDal(path string, pageSize int)(*dal,error)  {

	dal := &dal{
		meta:           newEmptyMeta(),
        pageSize: pageSize,
	}
    // the path is in memory
    if _,err:= os.Stat(path);err==nil{
        dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.close()
			return nil, err
		}
        meta,err := dal.readMeta()
        if err != nil {
            return nil, err
        }
        dal.meta = meta
        
        freeList, err := dal.readFreeList()
        if err !=nil{
            return nil,err
        }
        dal.freeList =freeList 
        
        
    }else if errors.Is(err, os.ErrNotExist){
        dal.file,err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
        if err != nil {
			_ = dal.close()
			return nil, err
		}
        dal.freeList = newFreeList()

        dal.freeListPage = dal.getNextPage()
        // leta write the current state to memory
        _, err := dal.writeFreeList()
		if err != nil {
			return nil, err
		}
		_, err = dal.writeMeta(dal.meta)

    }else {
		return nil, err
	}
    
    // }
    return dal,nil
}

// close file
func (d *dal) close() error  {
    if d.file !=nil{
        err:= d.file.Close()
        if err != nil{
            return fmt.Errorf("could not close file: %s",err)
        }
        d.file = nil
    }
    return nil
}

func (d *dal) allocateEmptyPage() *page{    
    return &page{
        data: make([]byte, d.pageSize),
    }
    
}


// Read the page, using offset to skip from memory
func (d *dal) readPage(pageNum pageNum) (*page,error)   {   

    p := d.allocateEmptyPage()

    offset := int(pageNum)*d.pageSize

    _,err := d.file.ReadAt(p.data,int64(offset))
    if err !=nil{
        return nil,err
    }

    return p,nil

    
}

// write the page on memory taking into consideration the offset
func (d *dal) writePage(p *page) error  {

    offset := int64(p.num) * int64(d.pageSize)
    _,err := d.file.WriteAt(p.data,offset)
    return err
    
}


// Get an Empty Page -> assign its pageNumber as 0(meta) -> serialize the data -> write on Page -> return page
func (d *dal) writeMeta(m *meta) (*page,error) {
    p := d.allocateEmptyPage()
    p.num = metaPageNum
    m.serialize(
        p.data,

    )

    err:= d.writePage(p)

    if err != nil{
        return nil,err
    }
    return p,nil

}


// read the meta page from memory -> take an empty meta page -> and deserialize the data data into the meta page
func (d *dal) readMeta() (*meta,error) {
 
    p,err := d.readPage(pageNum(metaPageNum))
    if err !=nil{
        return nil,err
    }
    metaPage := newEmptyMeta()
    metaPage.deserialize(
        p.data,
    )
    return metaPage,nil
}


// get an empty page -> assign its number as the the number of freeListPage, then serialize it -> write it to memory
func (d *dal) writeFreeList() (*page,error) {

    p:=d.allocateEmptyPage()

    p.num = d.freeListPage
    d.freeList.serialize(p.data)
    err :=d.writePage(p)
    if err !=nil{
        return nil,err
    }
    return p,err
    
}

func (d *dal) readFreeList() (*freeList,error) {
    // this freeListPage comes from meta so this function should never be called before creating a meta or reading an existing one
    p,err :=d.readPage(d.freeListPage)
    if err !=nil{
        return nil,err
    }

    freeList:=newFreeList()
    freeList.deserialize(p.data)
    return freeList,nil


}