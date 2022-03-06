package LibraDB

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

const (
	// The tests are designed so min items should be around 2 and max items should be around 4 (1 is not enough, 5 is
	// too much). The sizes are adjusted accordingly so those number will return in all tests.
	testPageSize      = 4096
	testMinPercentage = 0.2
	testMaxPercentage = 0.55
	testValSize       = 255
	
	mockNumberOfElements = 10
	expectedFolderPath   = "expected"
)

var (
	testCollectionName = []byte("test1")
)

func createTestDB(t *testing.T) (*DB, func()) {
	db, err := Open(getTempFileName(), &Options{MinFillPercent: testMinPercentage, MaxFillPercent: testMaxPercentage})
	require.NoError(t, err)

	return db, func() {
		_ = db.Close()
	}
}

func areCollectionsEqual(t *testing.T, c1, c2 *Collection) {
	assert.Equal(t, c1.name, c2.name)
	assert.Equal(t, c1.root, c2.root)
	assert.Equal(t, c1.counter, c2.counter)
}

func areTreesEqual(t *testing.T, t1, t2 *Collection) {
	t1Root, err := t1.tx.getNode(t1.root)
	require.NoError(t, err)

	t2Root, err := t2.tx.getNode(t2.root)
	require.NoError(t, err)

	areTreesEqualHelper(t, t1Root, t2Root)
}

func areNodesEqual(t *testing.T, n1, n2 *Node) {
	for i := 0; i < len(n1.items); i++ {
		assert.Equal(t, n1.items[i].key, n2.items[i].key)
		assert.Equal(t, n1.items[i].value, n2.items[i].value)
	}
}

func areTreesEqualHelper(t *testing.T, n1, n2 *Node) {
	require.Equal(t, len(n1.items), len(n2.items))
	require.Equal(t, len(n1.childNodes), len(n2.childNodes))
	areNodesEqual(t, n1, n2)
	// Exit condition: child node -> len(n1.childNodes) == 0
	for i := 0; i < len(n1.childNodes); i++ {
		node1, err := n1.tx.getNode(n1.childNodes[i])
		require.NoError(t, err)
		node2, err := n2.tx.getNode(n2.childNodes[i])
		areTreesEqualHelper(t, node1, node2)
	}
}

func createTestMockTree(t *testing.T) (*Collection, func()) {
	db, cleanFunc := createTestDB(t)

	tx := db.WriteTx()

	child0 := tx.writeNode(tx.newNode(createItems("0", "1"), []pgnum{}))

	child1 := tx.writeNode(tx.newNode(createItems("3", "4"), []pgnum{}))

	child2 := tx.writeNode(tx.newNode(createItems("6", "7", "8", "9"), []pgnum{}))

	root := tx.writeNode(tx.newNode(createItems("2", "5"), []pgnum{child0.pageNum, child1.pageNum, child2.pageNum}))

	expectedCollection, err := tx.createCollection(newCollection(testCollectionName, root.pageNum))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	return expectedCollection, cleanFunc
}

func getExpectedResultFileName(name string) string {
	return fmt.Sprintf("%s%c%s", expectedFolderPath, os.PathSeparator, name)
}

func getTempFileName() string {
	var id = uuid.New()
	return fmt.Sprintf("%s%c%s", os.TempDir(), os.PathSeparator,id)
}

func memset(buf []byte, count int) []byte {
	return bytes.Repeat(buf, count)
}

// createItem creates an item by memset a fixed size buf (255) with the given value. The fixed size is used so all tests
// so the minimum number of items in a node will be 2 and the maximum will be 4. This is for uniformity in the rebalance
// tests.
func createItem(key string) []byte {
	keyBuf := memset([]byte(key), testValSize)
	return keyBuf
}

func createItems(keys ...string) []*Item {
	items := make([]*Item, 0)
	for _, key := range keys {
		keyBuf := memset([]byte(key), testValSize)
		items = append(items, newItem(keyBuf, keyBuf))
	}
	return items
}
