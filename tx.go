package main

type tx struct {
	dirtyNodes    map[pgnum]*Node
	pagesToDelete []pgnum

	// new pages allocated during the transaction. They will be released if rollback is called.
	allocatedPageNums []pgnum

	write bool

	root pgnum
	db   *DB
}

func newTx(db *DB, root pgnum, write bool) *tx {
	return &tx{
		map[pgnum]*Node{},
		make([]pgnum, 0),
		make([]pgnum, 0),
		write,
		root,
		db,
	}
}

func (tx *tx) newNode(items []*Item, childNodes []pgnum) *Node {
	node := NewEmptyNode()
	node.items = items
	node.childNodes = childNodes
	node.pageNum = tx.db.getNextPage()
	node.tx = tx

	node.tx.allocatedPageNums = append(node.tx.allocatedPageNums, node.pageNum)
	return node
}

func (tx *tx) getNode(pageNum pgnum) (*Node, error) {
	if node, ok := tx.dirtyNodes[pageNum]; ok {
		return node, nil
	}

	node, err := tx.db.getNode(pageNum)
	if err != nil {
		return nil, err
	}
	node.tx = tx
	return node, nil
}

func (tx *tx) writeNode(node *Node) *Node {
	tx.dirtyNodes[node.pageNum] = node
	node.tx = tx
	return node
}

func (tx *tx) writeNodes(nodes ...*Node) {
	for _, node := range nodes {
		tx.writeNode(node)
	}
}

func (tx *tx) deleteNode(node *Node) {
	tx.pagesToDelete = append(tx.pagesToDelete, node.pageNum)
}

// This will be used for implementing COW. The idea is to mark all the dirty collection, then for each collection,
// traverse it's dirty in post order and commit child page. Then take the new page numbers, assign them to the parent,
// and save him as well and so. After we wrote all collections, rewrite the root node with the new collections roots.
// Rewrite the freelist with the newly allocated pages.  Finally, rewrite the meta page, so the new root node will take
// effect.
// COW will give us atomicity as new pages cannot be seen until the root node is written. This way, in case of a failure
// or a rollback no harm will be done as nothing was committed to the database.
//func (tx *tx) commitNode(node *Node) error {
//	oldPageNum := node.num
//	node.num = 0
//
//	newNode, err := tx.db.writeNode(node)
//	if err != nil {
//		return err
//	}
//	tx.committedNodes[oldPageNum] = newNode.num
//	tx.deleteNode(node)
//	return nil
//}
//
//// saveDirtyNodes saves the tree in a post order way. post order is used since child pages are written to the disk and
//// are given new page id, only then we can update the parent node with new page of the child node.
//func (tx *tx) saveDirtyNodes(node *Node) error {
//	if len(node.childNodes) == 0 {
//		return tx.commitNode(node)
//	}
//
//	for i, childNodePgid := range node.childNodes {
//		if childNode, ok := tx.dirtyNodes[childNodePgid]; ok {
//			err := tx.saveDirtyNodes(childNode)
//			if err != nil {
//				return err
//			}
//		}
//		node.childNodes[i] = tx.committedNodes[childNodePgid]
//	}
//
//	return tx.commitNode(node)
//}

func (tx *tx) getRootCollection() *Collection {
	rootCollection := newEmptyCollection()
	rootCollection.root = tx.root
	rootCollection.tx = tx
	return rootCollection
}

func (tx *tx) Rollback() {
	tx.dirtyNodes = nil
	tx.pagesToDelete = nil
	for _, pageNum := range tx.allocatedPageNums {
		tx.db.freelist.releasePage(pageNum)
	}
	tx.allocatedPageNums = nil
	tx.db.rwlock.Unlock()
}

func (tx *tx) Commit() error {
	for _, node := range tx.dirtyNodes {
		_, err := tx.db.writeNode(node)
		if err != nil {
			return err
		}
	}

	for _, pageNum := range tx.pagesToDelete {
		tx.db.deleteNode(pageNum)
	}
	freelistPage, err := tx.db.writeFreelist()
	if err != nil {
		return err
	}

	newMeta := newEmptyMeta()
	newMeta.root = tx.db.root
	newMeta.freelistPage = freelistPage.num
	_ , err = tx.db.writeMeta(newMeta)
	if err != nil {
		return err
	}

	tx.dirtyNodes = nil
	tx.pagesToDelete = nil
	tx.allocatedPageNums = nil
	if tx.write {
		tx.db.rwlock.Unlock()
	}
	return nil
}

func (tx *tx) GetCollection(name []byte) (*Collection, error) {
	rootCollection := tx.getRootCollection()
	item, err := rootCollection.Find(name)
	if err != nil {
		return nil, err
	}

	if item == nil {
		return nil, nil
	}

	collection := newEmptyCollection()
	collection.deserialize(item)
	collection.tx = tx
	return collection, nil
}

func (tx *tx) CreateCollection(name []byte) (*Collection, error) {
	rootCollection := tx.getRootCollection()

	newCollectionPage, err := tx.db.writeNode(NewEmptyNode())
	if err != nil {
		return nil, err
	}

	newCollection := newEmptyCollection()
	newCollection.name = name
	newCollection.root = newCollectionPage.pageNum
	newCollection.tx = tx
	collectionBytes := newCollection.serialize()

	err = rootCollection.Put(name, collectionBytes.value)
	if err != nil {
		return nil, err
	}

	return newCollection, nil
}

func (tx *tx) DeleteCollection(name []byte) error {
	rootCollection := tx.getRootCollection()

	return rootCollection.Remove(name)

}

func (tx *tx) createCollection(collection *Collection) (*Collection, error) {
	collection.tx = tx
	collectionBytes := collection.serialize()

	rootCollection := tx.getRootCollection()
	err := rootCollection.Put(collection.name, collectionBytes.value)
	if err != nil {
		return nil, err
	}

	return collection, nil
}
