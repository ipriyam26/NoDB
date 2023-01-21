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

type dal struct{
    file *os.File;
    pageSize int;
}

func newDal(path string, pageSize int)(*dal,error)  {
    file,err := os.OpenFile(path,os.O_RDWR|os.O_CREATE,0666)
    if err !=nil{
        return nil,err
    }
    dal :=&dal{
        file: file,
        pageSize: pageSize,
    }
    return dal,nil
}
func (d *dal) close() error  {
    if d.file !=nil{
        err:= d.file.Close()
        if err != nil{
            return fmt.Errorf("Could not Close file: %s",err)
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

func (d *dal) readPage(pageNum pageNum) (*page,error)   {   

    p := d.allocateEmptyPage()

    offset := int(pageNum)*d.pageSize

    _,err := d.file.ReadAt(p.data,int64(offset))
    if err !=nil{
        return nil,err
    }

    return p,nil

    
}

func (d *dal) writePage(p *page) error  {

    offset := int64(p.num) * int64(d.pageSize)
    _,err := d.file.WriteAt(p.data,offset)
    return err
    
}