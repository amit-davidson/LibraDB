package main

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func createTestDAL(t *testing.T) (*dal, func()) {
	fileName := getTempFileName()
	dal, err := newDal(fileName, &Options{
		pageSize: testPageSize,
	})
	require.NoError(t, err)

	cleanFunc := func() {
		err = dal.close()
		require.NoError(t, err)
		err = os.Remove(fileName)
		require.NoError(t, err)
	}
	return dal, cleanFunc
}

func TestCreateAndGetNode(t *testing.T) {
	dal, cleanFunc := createTestDAL(t)
	defer cleanFunc()

	items := []*Item{newItem([]byte("key1"), []byte("val1")), newItem([]byte("key2"), []byte("val2"))}
	var childNodes []pgnum

	expectedNode, err := dal.writeNode(NewNodeForSerialization(items, childNodes))
	require.NoError(t, err)

	actualNode, err := dal.getNode(expectedNode.pageNum)
	require.NoError(t, err)

	assert.Equal(t, expectedNode, actualNode)
}

func TestDeleteNode(t *testing.T) {
	dal, cleanFunc := createTestDAL(t)
	defer cleanFunc()

	var items []*Item
	var childNodes []pgnum

	node, err := dal.writeNode(NewNodeForSerialization(items, childNodes))
	require.NoError(t, err)
	assert.Equal(t, node.pageNum, dal.maxPage)

	dal.deleteNode(node.pageNum)

	assert.Equal(t, dal.releasedPages, []pgnum{node.pageNum})
	assert.Equal(t, node.pageNum, dal.maxPage)
}

func TestDeleteNodeAndReusePage(t *testing.T) {
	dal, cleanFunc := createTestDAL(t)
	defer cleanFunc()

	var items []*Item
	var childNodes []pgnum

	node, err := dal.writeNode(NewNodeForSerialization(items, childNodes))
	require.NoError(t, err)
	assert.Equal(t, node.pageNum, dal.maxPage)

	dal.deleteNode(node.pageNum)

	assert.Equal(t, dal.releasedPages, []pgnum{node.pageNum})
	assert.Equal(t, node.pageNum, dal.maxPage)

	newNode, err := dal.writeNode(NewNodeForSerialization(items, childNodes))
	require.NoError(t, err)
	assert.Equal(t, dal.releasedPages, []pgnum{})
	assert.Equal(t, newNode.pageNum, dal.maxPage)
}

func TestCreateDalWithNewFile(t *testing.T) {
	dal, cleanFunc := createTestDAL(t)
	defer cleanFunc()

	metaPage, err := dal.readMeta()
	require.NoError(t, err)

	freelistPageNum := pgnum(1)
	rootPageNum := pgnum(2)
	assert.Equal(t, freelistPageNum, metaPage.freelistPage)
	assert.Equal(t, rootPageNum, metaPage.root)

	assert.Equal(t, freelistPageNum, dal.freelistPage)
	assert.Equal(t, rootPageNum, dal.root)
}

func TestCreateDalWithExistingFile(t *testing.T) {
	// Make sure file exists
	_, err := os.Stat(getExpectedResultFileName(t.Name()))
	require.NoError(t, err)

	dal, cleanFunc := createTestDAL(t)
	defer cleanFunc()

	metaPage, err := dal.readMeta()
	require.NoError(t, err)

	freelistPageNum := pgnum(1)
	rootPageNum := pgnum(2)
	assert.Equal(t, freelistPageNum, metaPage.freelistPage)
	assert.Equal(t, rootPageNum, metaPage.root)

	assert.Equal(t, freelistPageNum, dal.freelistPage)
	assert.Equal(t, rootPageNum, dal.root)
}
