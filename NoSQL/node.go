package main

import "encoding/binary"

type Item struct{
	key []byte;
	value []byte;
}

type Node struct{
	*dal;
	pageNumNode pageNum;
	items []Item;
	childNodes	[]pageNum;
}

func newEmptyNode() *Node {
	return &Node{}
}

func newItem(key []byte,value []byte) *Item	  {
	return &Item{
		key: key,
		value: value,
	}
}

func (n *Node) isLeaf() bool{
	return len(n.childNodes) == 0
}

func (n *Node) serialize(buf []byte) []byte {

	leftPos :=0
	rightPos :=0
	 
	// Page Header
	isLeaf := n.isLeaf()
	var bitSetVar uint64
	if isLeaf{
		bitSetVar = 1
	}
	buf[leftPos] = byte(bitSetVar)
	leftPos+=1

	//count the number of key value pairs in the item
	binary.LittleEndian.AppendUint16(
		buf[leftPos:],
		uint16(len(n.items)),
	)
	leftPos+=2

	for i := 0; i < len(n.items); i++ {
		item := n.items[i]

		if !isLeaf{
			childNode := n.childNodes[i]
		
			//? we are storing the pageNumbers of childNodes

			// write as a fixed size of 8 bytes
			binary.LittleEndian.PutUint64(
				buf[leftPos:],
				uint64(childNode),
			)
			leftPos+=pageNumSize
		}
// Here is the bigger picture what
/*
We store the space taken by a offset caused a key value pair at the start, By doing this what we are enabling is
that when we have to look at any key we can go it directly from the index as everything if of same size of size*index will lead us the the memory spot of the the item and them at that item we can find the location of where the actual data is stored in which can be of any size, but as we have the offset we can go to in constant time. Hence giving us exact memory lookup of a value without actually, dealing the different sizes in data making it a linear task
*/
		keyLen := len(item.key)
		valueLen := len(item.value)

		//offset
		offset:=rightPos-keyLen-valueLen-2
		binary.LittleEndian.PutUint16(
			buf[leftPos:],
			uint16(offset),
		)
		leftPos+=2

		// we are subtracting valueLen from rightPos to make space for the new value
		rightPos-=valueLen
		// then we but the value in the buffer
		copy(buf[rightPos:],item.value)

		// we make one more space to store the length of the variable so that we can read it before the variable
		rightPos-=1
		buf[rightPos] = byte(valueLen)


		// same is done with the key
		rightPos-=keyLen
		copy(
			buf[rightPos:],
			item.key,
		)

		rightPos-=1
		buf[rightPos]=byte(keyLen)


	}

	if !isLeaf{

		lastChildNode := n.childNodes[len(n.childNodes)-1]

		binary.LittleEndian.PutUint64(buf[leftPos:],uint64(lastChildNode))
	}

return buf
	
}

func (n *Node) deserialize(buf []byte) {

	//reverse of deserialization

	leftPos :=0

	// Read Header
	isLeaf := uint16(buf[0])

	itemsCount := int(binary.LittleEndian.Uint16(buf[1:3]))

	for i := 0; i < itemsCount; i++ {
		
		if isLeaf ==0{
			pageNumber:= binary.LittleEndian.Uint64(buf[leftPos:])	
			leftPos += pageNumSize
			n.childNodes = append(n.childNodes, pageNum(pageNumber))
		}

		// Read offset
		offset := binary.LittleEndian.Uint16(buf[leftPos:])	
		leftPos+=2

		keyLen := uint16(buf[int(offset)])
		offset+=1

		key := buf[offset:offset+keyLen]
		offset+=keyLen

		valueLen := uint16(buf[int(offset)])
		offset+=1

		value := buf[offset:offset+valueLen]
		offset+=valueLen

		n.items = append(n.items, *newItem(key,value))
	}

	if isLeaf == 0 { // False
		// Read the last child node
		pageNum := pageNum(binary.LittleEndian.Uint64(buf[leftPos:]))
		n.childNodes = append(n.childNodes, pageNum)
	}
	
}