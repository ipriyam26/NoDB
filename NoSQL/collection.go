package main
type Collection struct {
	name []byte
	root pageNum

	dal *dal
}

func newCollection(name []byte, root pageNum) *Collection {
	return &Collection{
		name: name,
		root: root,
	}
}

func (c *Collection) Find(key []byte) (*Item,error){
	n, err := c.dal.getNode(c.root)
	if err != nil {
		return nil, err
	}

	index, containingNode,_,  err := n.findKey(key,true)
	if err != nil {
		return nil, err
	}
	if index == -1 {
		return nil, nil
	}
	return containingNode.items[index], nil
}