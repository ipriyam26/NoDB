package main

import (
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
}


// create a new DAL 
func newDal(path string, pageSize int)(*dal,error)  {
    file,err := os.OpenFile(path,os.O_RDWR|os.O_CREATE,0666)
    if err !=nil{
        return nil,err
    }
    dal :=&dal{
        file: file,
        pageSize: pageSize,
        freeList: newFreeList(),
    }
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


// Read the page, using offset to skip 
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

