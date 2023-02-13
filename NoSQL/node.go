package main

import (
	"bytes"
	"encoding/binary"
)

type Item struct{
	key []byte;
	value []byte;
}

type Node struct{
	*dal;
	pageNumNode pageNum;
	items []*Item;
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

		n.items = append(n.items, newItem(key,value))
	}

	if isLeaf == 0 { // False
		// Read the last child node
		pageNum := pageNum(binary.LittleEndian.Uint64(buf[leftPos:]))
		n.childNodes = append(n.childNodes, pageNum)
	}
	

}


func (n *Node) writeNode(node *Node)( *Node,error) {

	newNode,err := n.dal.writeNode(node)
	if err !=nil{
		return nil,err
	}
	return newNode,nil
	
}

func (n *Node) writeNodes(nodes ...*Node) {
	for _, node := range nodes {
		n.writeNode(node)
	}
}

func (n *Node) getNode(pgnum pageNum) (*Node, error) {
	return n.dal.getNode(pgnum)
}

//Todo: use binary search here instead of this linear one
func (n *Node) findKeyInNode(key []byte) (bool,int){
	for i, existingItem := range n.items {
		res := bytes.Compare(existingItem.key,key)
		if res ==0{
			return true,i
		}

		if res==1{
			return false,i
		}
	}
	return false, len(n.items)

}

func (n *Node) findKey(key []byte, exact bool) (int, *Node, []int ,error) {
	ancestorsIndexes := []int{0} // index of root
	index, node, err := findKeyHelper(n, key, exact, &ancestorsIndexes)
	if err != nil {
		return -1, nil, nil, err
	}
	return index, node, ancestorsIndexes, nil
}



func findKeyHelper(node *Node, key []byte, exact bool, ancestorsIndexes *[]int) (int, *Node ,error) {
	wasFound, index := node.findKeyInNode(key)
	if wasFound {
		return index, node, nil
	}

	if node.isLeaf() {
		if exact {
			return -1, nil, nil
		}
		return index, node, nil
	}

	*ancestorsIndexes = append(*ancestorsIndexes, index)
	nextChild, err := node.getNode(node.childNodes[index])
	if err != nil {
		return -1, nil, err
	}
	return findKeyHelper(nextChild, key, exact, ancestorsIndexes)
}

// elementSize returns the size of a key-value-childNode triplet at a given index.
// If the node is a leaf, then the size of a key-value pair is returned. 
// It's assumed i <= len(n.items)
func (n *Node) elementSize(i int) int {
	size := 0
	size += len(n.items[i].key)
	size += len(n.items[i].value)
	size += pageNumSize // 8 is the pgnum size
	return size
}

// nodeSize returns the node's size in bytes
func (n *Node) nodeSize() int {
	size := 0
	size += nodeHeaderSize

	for i := range n.items {
		size += n.elementSize(i)
	}

	// Add last page
	size += pageNumSize // 8 is the pgnum size
	return size
}
func (n *Node) addItem(item *Item, index int) int {
	if index == len(n.items){
		n.items = append(n.items, item)
		return index
	}

	// lets empty the index position in n.items
	n.items = append(n.items[:index+1], n.items[index:]...)

	n.items[index] = item
	return index
}
func (n *Node) isOverPopulated() bool {
	return n.dal.isOverPopulated(n)
}

// isUnderPopulated checks if the node size is smaller than the size of a page.
func (n *Node) isUnderPopulated() bool {
	return n.dal.isUnderPopulated(n)
}

func (n *Node) split(nodeToSplit *Node,nodeToSplitIndex int) {
	splitIndex:=nodeToSplit.getSplitIndex(nodeToSplit)
	middleItem := nodeToSplit.items[splitIndex]
	var newNode *Node

	if nodeToSplit.isLeaf(){
		newNode,_ = n.writeNode(
			n.dal.newNode(nodeToSplit.items[splitIndex+1:],
				[]pageNum{}))
				nodeToSplit.items = nodeToSplit.items[:splitIndex]
	}else{
		newNode, _ = n.writeNode(n.dal.newNode(nodeToSplit.items[splitIndex+1:], nodeToSplit.childNodes[splitIndex+1:]))
		nodeToSplit.items = nodeToSplit.items[:splitIndex]
		nodeToSplit.childNodes = nodeToSplit.childNodes[:splitIndex+1]
	}
	n.addItem(middleItem,nodeToSplitIndex)

	if len(n.childNodes) == nodeToSplitIndex+1 { // If middle of list, then move items forward
		n.childNodes = append(n.childNodes, newNode.pageNumNode)
	} else {
		n.childNodes = append(n.childNodes[:nodeToSplitIndex+1], n.childNodes[nodeToSplitIndex:]...)
		n.childNodes[nodeToSplitIndex+1] = newNode.pageNumNode
	}

	n.writeNodes(n, nodeToSplit)
}