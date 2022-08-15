package LibraDB

import (
	"errors"
	"fmt"
	"os"
)

type pgnum uint64

type Options struct {
	pageSize int

	MinFillPercent float32
	MaxFillPercent float32
}

var DefaultOptions = &Options{
	MinFillPercent: 0.5,
	MaxFillPercent: 0.95,
}

type page struct {
	num  pgnum
	data []byte
}

type dal struct {
	pageSize       int
	minFillPercent float32
	maxFillPercent float32
	file           *os.File

	*meta
	*freelist
}

func newDal(path string, options *Options) (*dal, error) {
	dal := &dal{
		meta:           newEmptyMeta(),
		pageSize:       options.pageSize,
		minFillPercent: options.MinFillPercent,
		maxFillPercent: options.MaxFillPercent,
	}

	// exist
	if _, err := os.Stat(path); err == nil {
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.close()
			return nil, err
		}

		meta, err := dal.readMeta()
		if err != nil {
			return nil, err
		}
		dal.meta = meta

		freelist, err := dal.readFreelist()
		if err != nil {
			return nil, err
		}
		dal.freelist = freelist
		// doesn't exist
	} else if errors.Is(err, os.ErrNotExist) {
		// init freelist
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.close()
			return nil, err
		}

		dal.freelist = newFreelist()
		_, err := dal.writeFreelist()
		if err != nil {
			return nil, err
		}

		// init root
		collectionsNode, err := dal.writeNode(NewNodeForSerialization([]*Item{}, []pgnum{}))
		if err != nil {
			return nil, err
		}
		dal.root = collectionsNode.pageNum

		// write meta page
		_, err = dal.writeMeta(dal.meta) // other error
	} else {
		return nil, err
	}
	return dal, nil
}

// getSplitIndex should be called when performing rebalance after an item is removed. It checks if a node can spare an
// element, and if it does then it returns the index when there the split should happen. Otherwise -1 is returned.
func (d *dal) getSplitIndex(node *Node) int {
	size := 0
	size += nodeHeaderSize

	for i := range node.items {
		size += node.elementSize(i)

		// if we have a big enough page size (more than minimum), and didn't reach the last node, which means we can
		// spare an element
		if float32(size) > d.minThreshold() && i < len(node.items)-1 {
			return i + 1
		}
	}

	return -1
}

func (d *dal) maxThreshold() float32 {
	return d.maxFillPercent * float32(d.pageSize)
}

func (d *dal) isOverPopulated(node *Node) bool {
	return float32(node.nodeSize()) > d.maxThreshold()
}

func (d *dal) minThreshold() float32 {
	return d.minFillPercent * float32(d.pageSize)
}

func (d *dal) isUnderPopulated(node *Node) bool {
	return float32(node.nodeSize()) < d.minThreshold()
}

func (d *dal) close() error {
	if d.file != nil {
		err := d.file.Close()
		if err != nil {
			return fmt.Errorf("could not close file: %s", err)
		}
		d.file = nil
	}

	return nil
}

func (d *dal) allocateEmptyPage() *page {
	return &page{
		data: make([]byte, d.pageSize, d.pageSize),
	}
}

func (d *dal) readPage(pageNum pgnum) (*page, error) {
	p := d.allocateEmptyPage()

	offset := int(pageNum) * d.pageSize
	_, err := d.file.ReadAt(p.data, int64(offset))
	if err != nil {
		return nil, err
	}
	return p, err
}

func (d *dal) writePage(p *page) error {
	offset := int64(p.num) * int64(d.pageSize)
	_, err := d.file.WriteAt(p.data, offset)
	return err
}

func (d *dal) getNode(pageNum pgnum) (*Node, error) {
	p, err := d.readPage(pageNum)
	if err != nil {
		return nil, err
	}
	node := NewEmptyNode()
	node.deserialize(p.data)
	node.pageNum = pageNum
	return node, nil
}

func (d *dal) writeNode(n *Node) (*Node, error) {
	p := d.allocateEmptyPage()
	if n.pageNum == 0 {
		p.num = d.getNextPage()
		n.pageNum = p.num
	} else {
		p.num = n.pageNum
	}

	p.data = n.serialize(p.data)

	err := d.writePage(p)
	if err != nil {
		return nil, err
	}
	return n, nil
}

func (d *dal) deleteNode(pageNum pgnum) {
	d.releasePage(pageNum)
}

func (d *dal) readFreelist() (*freelist, error) {
	p, err := d.readPage(d.freelistPage)
	if err != nil {
		return nil, err
	}

	freelist := newFreelist()
	freelist.deserialize(p.data)
	return freelist, nil
}

// writeFreelist first marks the previous freelist page as deleted, then proceeds to request a new page.
func (d *dal) writeFreelist() (*page, error) {
	d.deleteNode(d.freelistPage)

	p := d.allocateEmptyPage()
	p.num = d.getNextPage()
	d.freelist.serialize(p.data)

	err := d.writePage(p)
	if err != nil {
		return nil, err
	}
	d.freelistPage = p.num
	return p, nil
}

func (d *dal) writeMeta(meta *meta) (*page, error) {
	p := d.allocateEmptyPage()
	p.num = metaPageNum
	meta.serialize(p.data)

	err := d.writePage(p)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (d *dal) readMeta() (*meta, error) {
	p, err := d.readPage(metaPageNum)
	if err != nil {
		return nil, err
	}

	meta := newEmptyMeta()
	meta.deserialize(p.data)
	return meta, nil
}
