package LibraDB

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTx_CreateCollection(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()
	collection, err := tx.CreateCollection(testCollectionName)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	tx = db.ReadTx()
	actualCollection, err := tx.GetCollection(collection.name)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	areCollectionsEqual(t, collection, actualCollection)
}

func TestTx_Rollback(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()
	child0 := tx.writeNode(tx.newNode(createItems("0", "1", "2", "3"), []pgnum{}))

	child1 := tx.writeNode(tx.newNode(createItems("5", "6", "7", "8"), []pgnum{}))

	root := tx.writeNode(tx.newNode(createItems("4"), []pgnum{child0.pageNum, child1.pageNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pageNum))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	// Try to add 9 but then perform a rollback, so it won't be saved
	tx2 := db.WriteTx()

	collection, err = tx2.GetCollection(collection.name)
	require.NoError(t, err)

	val := createItem("9")
	err = collection.Put(val, val)
	require.NoError(t, err)

	tx2.Rollback()

	// 9 should not exist. Also, a new page should be added to released page ids.
	tx3 := db.ReadTx()
	assert.Len(t, tx3.db.freelist.releasedPages, 1)

	collection, err = tx3.GetCollection(collection.name)
	require.NoError(t, err)

	// Item not found
	expectedVal := createItem("9")
	item, err := collection.Find(expectedVal)
	require.NoError(t, err)
	assert.Nil(t, item)

	err = tx3.Commit()
	require.NoError(t, err)

	assert.Len(t, tx3.db.freelist.releasedPages, 0)
}
