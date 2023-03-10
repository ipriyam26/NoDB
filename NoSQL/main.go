package main

import "os"

func main() {
	// initialize db
	dal, _ := newDal("db.db", os.Getpagesize())

	// create a new page
	p := dal.allocateEmptyPage()
	p.num = dal.getNextPage()
	copy(p.data[:], "data")

	// commit it
	_ = dal.writePage(p)
	_, _ = dal.writeFreeList()

	// Close the db
	_ = dal.close()


	// We expect the freeList state was saved, so we write to
	// page number 3 and not overwrite the one at number 2
	dal, _ = newDal("db.db",os.Getpagesize())
	p = dal.allocateEmptyPage()
	p.num = dal.getNextPage()
	copy(p.data[:], "data2")
	_ = dal.writePage(p)

	// Create a page and free it so the released pages will be updated
	pageNum := dal.getNextPage()
	dal.releasePage(pageNum)

	// commit it
	_, _ = dal.writeFreeList()

}