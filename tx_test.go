package LibraDB

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sync"
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

func TestTx_CreateCollectionReadTx(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.ReadTx()
	collection, err := tx.CreateCollection(testCollectionName)
	require.Error(t, err)
	require.Nil(t, collection)

	err = tx.Commit()
}

func TestTx_OpenMultipleReadTxSimultaneously(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx1 := db.ReadTx()
	tx2 := db.ReadTx()

	collection1, err := tx1.GetCollection(testCollectionName)
	require.NoError(t, err)
	require.Nil(t, collection1)

	collection2, err := tx2.GetCollection(testCollectionName)
	require.NoError(t, err)
	require.Nil(t, collection2)

	err = tx1.Commit()
	err = tx2.Commit()
}

// TestTx_OpenReadAndWriteTxSimultaneously Validates read and write tx don't run simultaneously. It first starts a read
// tx, then once the lock was acquired, a write transaction is started. Once the read tx finishes, the lock is released,
// and the write tx start executing. Once the lock is acquired again, a read tx is triggered.
// The nesting of the functions is done to make sure the transactions are fired only after the lock is acquired to avoid
// race conditions.
// We expect the first read tx (tx1) not to see the changes done by the write tx (tx2) even when though it queries the database
// after the write tx was triggered (Because of the lock).
// And again, even though tx3 was triggered before tx2 committed the changes, it doesn't query the database yet since
// the lock is already acquired. It will query the database only after tx2 makes the changes and releases the lock.
func TestTx_OpenReadAndWriteTxSimultaneously(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	wg := sync.WaitGroup{}

	tx1 := db.ReadTx()

	// Start write tx only after the lock was acquired by the read tx
	wg.Add(1)
	go func() {
		tx2 := db.WriteTx()

		// Start read tx only after the lock was acquired by the write tx
		wg.Add(1)
		go func() {
			tx3 := db.ReadTx()

			collection3, err := tx3.GetCollection(testCollectionName)
			require.NoError(t, err)
			require.Equal(t, testCollectionName, collection3.name)

			err = tx3.Commit()
			wg.Done()
		}()


		_, err := tx2.CreateCollection(testCollectionName)
		require.NoError(t, err)

		err = tx2.Commit()
		wg.Done()
	}()

	collection1, err := tx1.GetCollection(testCollectionName)
	require.NoError(t, err)
	require.Nil(t, collection1)

	err = tx1.Commit()

	wg.Wait()
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

	assert.Len(t, tx.db.freelist.releasedPages, 0)

	// Try to add 9 but then perform a rollback, so it won't be saved
	tx2 := db.WriteTx()

	collection, err = tx2.GetCollection(collection.name)
	require.NoError(t, err)

	val := createItem("9")
	err = collection.Put(val, val)
	require.NoError(t, err)

	tx2.Rollback()

	// 9 should not exist since a rollback was performed. A new page should be added to released page ids though, since
	// a split occurred and a new page node was created, but later deleted.
	assert.Len(t, tx2.db.freelist.releasedPages, 1)
	tx3 := db.ReadTx()

	collection, err = tx3.GetCollection(collection.name)
	require.NoError(t, err)

	// Item not found
	expectedVal := createItem("9")
	item, err := collection.Find(expectedVal)
	require.NoError(t, err)
	assert.Nil(t, item)

	err = tx3.Commit()
	require.NoError(t, err)

	assert.Len(t, tx3.db.freelist.releasedPages, 1)
}
