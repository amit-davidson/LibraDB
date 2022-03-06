package LibraDB

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"strconv"
	"testing"
)

func Test_AddSingle(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()
	collection, err := tx.CreateCollection(testCollectionName)
	require.NoError(t, err)

	value := createItem("0")
	err = collection.Put(value, value)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTx := expectedDB.WriteTx()

	expectedRoot := expectedTx.writeNode(expectedTx.newNode(createItems("0"), []pgnum{}))
	expectedCollection, err := expectedTx.createCollection(newCollection(testCollectionName, expectedRoot.pageNum))
	require.NoError(t, err)

	err = expectedTx.Commit()
	require.NoError(t, err)

	areTreesEqual(t, expectedCollection, collection)
}

func Test_RemoveFromRootSingleElement(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()
	collection, err := tx.CreateCollection(testCollectionName)
	require.NoError(t, err)

	value := createItem("0")
	err = collection.Put(value, value)
	require.NoError(t, err)

	err = collection.Remove(value)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDBAfterRemoval, cleanFuncAfterRemoval := createTestDB(t)
	defer cleanFuncAfterRemoval()

	expectedTxAfterRemoval := expectedDBAfterRemoval.WriteTx()

	expectedRootAfterRemoval := expectedTxAfterRemoval.writeNode(expectedTxAfterRemoval.newNode([]*Item{}, []pgnum{}))

	expectedCollectionAfterRemoval, err := expectedTxAfterRemoval.createCollection(newCollection(testCollectionName, expectedRootAfterRemoval.pageNum))

	err = expectedTxAfterRemoval.Commit()
	require.NoError(t, err)

	areTreesEqual(t, expectedCollectionAfterRemoval, collection)
}

func Test_AddMultiple(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()
	collection, err := tx.CreateCollection(testCollectionName)
	require.NoError(t, err)

	numOfElements := mockNumberOfElements
	for i := 0; i < numOfElements; i++ {
		val := createItem(strconv.Itoa(i))
		err = collection.Put(val, val)
		require.NoError(t, err)
	}
	err = tx.Commit()
	require.NoError(t, err)

	// Tree is balanced
	expected, expectedCleanFunc := createTestMockTree(t)
	defer expectedCleanFunc()
	areTreesEqual(t, expected, collection)
}

func Test_AddAndRebalanceSplit(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()
	child0 := tx.writeNode(tx.newNode(createItems("0", "1", "2", "3"), []pgnum{}))

	child1 := tx.writeNode(tx.newNode(createItems("5", "6", "7", "8"), []pgnum{}))

	root := tx.writeNode(tx.newNode(createItems("4"), []pgnum{child0.pageNum, child1.pageNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pageNum))
	require.NoError(t, err)

	val := createItem("9")
	err = collection.Put(val, val)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedTestDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	testTx := expectedTestDB.WriteTx()

	expectedChild0 := testTx.writeNode(testTx.newNode(createItems("0", "1", "2", "3"), []pgnum{}))

	expectedChild1 := testTx.writeNode(testTx.newNode(createItems("5", "6"), []pgnum{}))

	expectedChild2 := testTx.writeNode(testTx.newNode(createItems("8", "9"), []pgnum{}))

	expectedRoot := testTx.writeNode(testTx.newNode(createItems("4", "7"), []pgnum{expectedChild0.pageNum, expectedChild1.pageNum, expectedChild2.pageNum}))

	expectedCollection, err := testTx.createCollection(newCollection(testCollectionName, expectedRoot.pageNum))
	require.NoError(t, err)

	err = testTx.Commit()
	require.NoError(t, err)

	// Tree is balanced
	areTreesEqual(t, expectedCollection, collection)
}

func Test_SplitAndMerge(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()
	child0 := tx.writeNode(tx.newNode(createItems("0", "1", "2", "3"), []pgnum{}))

	child1 := tx.writeNode(tx.newNode(createItems("5", "6", "7", "8"), []pgnum{}))

	root := tx.writeNode(tx.newNode(createItems("4"), []pgnum{child0.pageNum, child1.pageNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pageNum))
	require.NoError(t, err)

	val := createItem("9")
	err = collection.Put(val, val)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedTestDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	testTx := expectedTestDB.WriteTx()

	expectedChild0 := testTx.writeNode(testTx.newNode(createItems("0", "1", "2", "3"), []pgnum{}))

	expectedChild1 := testTx.writeNode(testTx.newNode(createItems("5", "6"), []pgnum{}))

	expectedChild2 := testTx.writeNode(testTx.newNode(createItems("8", "9"), []pgnum{}))

	expectedRoot := testTx.writeNode(testTx.newNode(createItems("4", "7"), []pgnum{expectedChild0.pageNum, expectedChild1.pageNum, expectedChild2.pageNum}))

	expectedCollection, err := testTx.createCollection(newCollection(testCollectionName, expectedRoot.pageNum))
	require.NoError(t, err)

	// Tree is balanced
	areTreesEqual(t, expectedCollection, collection)

	err = testTx.Commit()
	require.NoError(t, err)

	removeTx := db.WriteTx()
	collection , err = removeTx.GetCollection(collection.name)
	require.NoError(t, err)

	err = collection.Remove(val)
	require.NoError(t, err)

	err = removeTx.Commit()
	require.NoError(t, err)

	expectedDBAfterRemoval, expectedDBCleanFunc := createTestDB(t)
	defer expectedDBCleanFunc()

	expectedTxAfterRemoval := expectedDBAfterRemoval.WriteTx()
	expectedChild0AfterRemoval := expectedTxAfterRemoval.writeNode(expectedTxAfterRemoval.newNode(createItems("0", "1", "2", "3"), []pgnum{}))

	expectedChild1AfterRemoval := expectedTxAfterRemoval.writeNode(expectedTxAfterRemoval.newNode(createItems("5", "6", "7", "8"), []pgnum{}))

	expectedRootAfterRemoval := expectedTxAfterRemoval.writeNode(expectedTxAfterRemoval.newNode(createItems("4"), []pgnum{expectedChild0AfterRemoval.pageNum, expectedChild1AfterRemoval.pageNum}))

	expectedCollectionAfterRemoval, err := expectedTxAfterRemoval.createCollection(newCollection(testCollectionName, expectedRootAfterRemoval.pageNum))
	require.NoError(t, err)

	err = expectedTxAfterRemoval.Commit()
	require.NoError(t, err)

	areTreesEqual(t, expectedCollectionAfterRemoval, collection)
}

func Test_RemoveFromRootWithoutRebalance(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()
	collection, err := tx.CreateCollection(testCollectionName)
	require.NoError(t, err)

	numOfElements := mockNumberOfElements
	for i := 0; i < numOfElements; i++ {
		val := createItem(strconv.Itoa(i))
		err = collection.Put(val, val)
		require.NoError(t, err)
	}

	// Remove an element
	err = collection.Remove(createItem("7"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTestTx := expectedDB.WriteTx()

	expectedChild0 := expectedTestTx.writeNode(expectedTestTx.newNode(createItems("0", "1"), []pgnum{}))

	expectedChild1 := expectedTestTx.writeNode(expectedTestTx.newNode(createItems("3", "4"), []pgnum{}))

	expectedChild2 := expectedTestTx.writeNode(expectedTestTx.newNode(createItems("6", "8", "9"), []pgnum{}))

	expectedRoot := expectedTestTx.writeNode(expectedTestTx.newNode(createItems("2", "5"), []pgnum{expectedChild0.pageNum, expectedChild1.pageNum, expectedChild2.pageNum}))

	expectedCollectionAfterRemoval, err := expectedTestTx.createCollection(newCollection(testCollectionName, expectedRoot.pageNum))
	require.NoError(t, err)

	err = expectedTestTx.Commit()
	require.NoError(t, err)

	areTreesEqual(t, expectedCollectionAfterRemoval, collection)
}

func Test_RemoveFromRootAndRotateLeft(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child0 := tx.writeNode(tx.newNode(createItems("0", "1"), []pgnum{}))

	child1 := tx.writeNode(tx.newNode(createItems("3", "4"), []pgnum{}))

	child2 := tx.writeNode(tx.newNode(createItems("6", "7", "8"), []pgnum{}))

	root := tx.writeNode(tx.newNode(createItems("2", "5"), []pgnum{child0.pageNum, child1.pageNum, child2.pageNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pageNum))
	require.NoError(t, err)

	// Remove an element
	err = collection.Remove(createItem("5"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTx := expectedDB.WriteTx()

	expectedChild0 := expectedTx.writeNode(expectedTx.newNode(createItems("0", "1"), []pgnum{}))

	expectedChild1 := expectedTx.writeNode(expectedTx.newNode(createItems("3", "4"), []pgnum{}))

	expectedChild2 := expectedTx.writeNode(expectedTx.newNode(createItems("7", "8"), []pgnum{}))

	expectedRoot := expectedTx.writeNode(expectedTx.newNode(createItems("2", "6"), []pgnum{expectedChild0.pageNum, expectedChild1.pageNum, expectedChild2.pageNum}))

	expectedCollection, err := expectedTx.createCollection(newCollection(testCollectionName, expectedRoot.pageNum))
	require.NoError(t, err)

	err = expectedTx.Commit()
	require.NoError(t, err)

	areTreesEqual(t, expectedCollection, collection)
}

func Test_RemoveFromRootAndRotateRight(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child0 := tx.writeNode(tx.newNode(createItems("0", "1", "2"), []pgnum{}))

	child1 := tx.writeNode(tx.newNode(createItems("4", "5"), []pgnum{}))

	child2 := tx.writeNode(tx.newNode(createItems("7", "8"), []pgnum{}))

	root := tx.writeNode(tx.newNode(createItems("3", "6"), []pgnum{child0.pageNum, child1.pageNum, child2.pageNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pageNum))
	require.NoError(t, err)

	// Remove an element
	err = collection.Remove(createItem("6"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTx := expectedDB.WriteTx()

	expectedChild0 := expectedTx.writeNode(expectedTx.newNode(createItems("0", "1"), []pgnum{}))

	expectedChild1 := expectedTx.writeNode(expectedTx.newNode(createItems("3", "4"), []pgnum{}))

	expectedChild2 := expectedTx.writeNode(expectedTx.newNode(createItems("7", "8"), []pgnum{}))

	expectedRoot := expectedTx.writeNode(expectedTx.newNode(createItems("2", "5"), []pgnum{expectedChild0.pageNum, expectedChild1.pageNum, expectedChild2.pageNum}))

	expectedCollection, err := expectedTx.createCollection(newCollection(testCollectionName, expectedRoot.pageNum))
	require.NoError(t, err)

	err = expectedTx.Commit()
	require.NoError(t, err)

	areTreesEqual(t, expectedCollection, collection)
}

// Test_RemoveFromRootAndRebalanceMergeToUnbalanced tests when the unbalanced node is the most left one so the
// merge has to happen from the right node into the unbalanced node
func Test_RemoveFromRootAndRebalanceMergeToUnbalanced(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child0 := tx.writeNode(tx.newNode(createItems("0", "1"), []pgnum{}))

	child1 := tx.writeNode(tx.newNode(createItems("3", "4"), []pgnum{}))

	child2 := tx.writeNode(tx.newNode(createItems("6", "7"), []pgnum{}))

	root := tx.writeNode(tx.newNode(createItems("2", "5"), []pgnum{child0.pageNum, child1.pageNum, child2.pageNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pageNum))
	require.NoError(t, err)

	// Remove an element
	err = collection.Remove(createItem("2"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTx := expectedDB.WriteTx()

	expectedChild0 := expectedTx.writeNode(expectedTx.newNode(createItems("0", "1", "3", "4"), []pgnum{}))

	expectedChild1 := expectedTx.writeNode(expectedTx.newNode(createItems("6", "7"), []pgnum{}))

	expectedRoot := expectedTx.writeNode(expectedTx.newNode(createItems("5"), []pgnum{expectedChild0.pageNum, expectedChild1.pageNum}))

	expectedCollection, err := expectedTx.createCollection(newCollection(testCollectionName, expectedRoot.pageNum))
	require.NoError(t, err)

	err = expectedTx.Commit()
	require.NoError(t, err)

	areTreesEqual(t, expectedCollection, collection)
}

// Test_RemoveFromRootAndRebalanceMergeFromUnbalanced tests when the unbalanced node is not the most left one so the
// merge has to happen from the unbalanced node to the node left to it
func Test_RemoveFromRootAndRebalanceMergeFromUnbalanced(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child0 := tx.writeNode(tx.newNode(createItems("0", "1"), []pgnum{}))

	child1 := tx.writeNode(tx.newNode(createItems("3", "4"), []pgnum{}))

	child2 := tx.writeNode(tx.newNode(createItems("6", "7"), []pgnum{}))

	root := tx.writeNode(tx.newNode(createItems("2", "5"), []pgnum{child0.pageNum, child1.pageNum, child2.pageNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pageNum))
	require.NoError(t, err)

	// Remove an element
	err = collection.Remove(createItem("5"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTx := expectedDB.WriteTx()

	expectedChild0 := expectedTx.writeNode(expectedTx.newNode(createItems("0", "1", "2", "3"), []pgnum{}))

	expectedChild1 := expectedTx.writeNode(expectedTx.newNode(createItems("6", "7"), []pgnum{}))

	expectedRoot := expectedTx.writeNode(expectedTx.newNode(createItems("4"), []pgnum{expectedChild0.pageNum, expectedChild1.pageNum}))

	expectedCollection, err := expectedTx.createCollection(newCollection(testCollectionName, expectedRoot.pageNum))
	require.NoError(t, err)

	err = expectedTx.Commit()
	require.NoError(t, err)

	// Remove an element
	areTreesEqual(t, expectedCollection, collection)
}

func Test_RemoveFromInnerNodeAndRotateLeft(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child00 := tx.writeNode(tx.newNode(createItems("0", "1"), []pgnum{}))

	child01 := tx.writeNode(tx.newNode(createItems("3", "4"), []pgnum{}))

	child02 := tx.writeNode(tx.newNode(createItems("6", "7"), []pgnum{}))

	child0 := tx.writeNode(tx.newNode(createItems("2", "5"), []pgnum{child00.pageNum, child01.pageNum, child02.pageNum}))

	child10 := tx.writeNode(tx.newNode(createItems("9", "a"), []pgnum{}))

	child11 := tx.writeNode(tx.newNode(createItems("c", "d"), []pgnum{}))

	child12 := tx.writeNode(tx.newNode(createItems("f", "g"), []pgnum{}))

	child13 := tx.writeNode(tx.newNode(createItems("i", "j"), []pgnum{}))

	child1 := tx.writeNode(tx.newNode(createItems("b", "e", "h"), []pgnum{child10.pageNum, child11.pageNum, child12.pageNum, child13.pageNum}))

	root := tx.writeNode(tx.newNode(createItems("8"), []pgnum{child0.pageNum, child1.pageNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pageNum))
	require.NoError(t, err)

	// Remove an element
	err = collection.Remove(createItem("5"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTx := expectedDB.WriteTx()

	expectedChild00 := expectedTx.writeNode(expectedTx.newNode(createItems("0", "1", "2", "3"), []pgnum{}))

	expectedChild01 := expectedTx.writeNode(expectedTx.newNode(createItems("6", "7"), []pgnum{}))

	expectedChild02 := expectedTx.writeNode(expectedTx.newNode(createItems("9", "a"), []pgnum{}))

	expectedChild0 := expectedTx.writeNode(expectedTx.newNode(createItems("4", "8"), []pgnum{expectedChild00.pageNum, expectedChild01.pageNum, expectedChild02.pageNum}))

	expectedChild10 := expectedTx.writeNode(expectedTx.newNode(createItems("c", "d"), []pgnum{}))

	expectedChild11 := expectedTx.writeNode(expectedTx.newNode(createItems("f", "g"), []pgnum{}))

	expectedChild12 := expectedTx.writeNode(expectedTx.newNode(createItems("i", "j"), []pgnum{}))

	expectedChild1 := expectedTx.writeNode(expectedTx.newNode(createItems("e", "h"), []pgnum{expectedChild10.pageNum, expectedChild11.pageNum, expectedChild12.pageNum}))

	expectedRoot := expectedTx.writeNode(expectedTx.newNode(createItems("b"), []pgnum{expectedChild0.pageNum, expectedChild1.pageNum}))

	expectedCollection, err := expectedTx.createCollection(newCollection(testCollectionName, expectedRoot.pageNum))
	require.NoError(t, err)

	err = expectedTx.Commit()
	require.NoError(t, err)

	// Remove an element
	areTreesEqual(t, expectedCollection, collection)
}

func Test_RemoveFromInnerNodeAndRotateRight(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child00 := tx.writeNode(tx.newNode(createItems("0", "1"), []pgnum{}))

	child01 := tx.writeNode(tx.newNode(createItems("3", "4"), []pgnum{}))

	child02 := tx.writeNode(tx.newNode(createItems("6", "7"), []pgnum{}))

	child03 := tx.writeNode(tx.newNode(createItems("9", "a"), []pgnum{}))

	child0 := tx.writeNode(tx.newNode(createItems("2", "5", "8"), []pgnum{child00.pageNum, child01.pageNum, child02.pageNum, child03.pageNum}))

	child10 := tx.writeNode(tx.newNode(createItems("c", "d"), []pgnum{}))

	child11 := tx.writeNode(tx.newNode(createItems("f", "g"), []pgnum{}))

	child12 := tx.writeNode(tx.newNode(createItems("i", "j"), []pgnum{}))

	child1 := tx.writeNode(tx.newNode(createItems("e", "h"), []pgnum{child10.pageNum, child11.pageNum, child12.pageNum}))

	root := tx.writeNode(tx.newNode(createItems("b"), []pgnum{child0.pageNum, child1.pageNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pageNum))
	require.NoError(t, err)

	// Remove an element
	err = collection.Remove(createItem("e"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTx := expectedDB.WriteTx()

	expectedChild00 := expectedTx.writeNode(expectedTx.newNode(createItems("0", "1"), []pgnum{}))

	expectedChild01 := expectedTx.writeNode(expectedTx.newNode(createItems("3", "4"), []pgnum{}))

	expectedChild02 := expectedTx.writeNode(expectedTx.newNode(createItems("6", "7"), []pgnum{}))

	expectedChild0 := expectedTx.writeNode(expectedTx.newNode(createItems("2", "5"), []pgnum{expectedChild00.pageNum, expectedChild01.pageNum, expectedChild02.pageNum}))

	expectedChild10 := expectedTx.writeNode(expectedTx.newNode(createItems("9", "a"), []pgnum{}))

	expectedChild11 := expectedTx.writeNode(expectedTx.newNode(createItems("c", "d", "f", "g"), []pgnum{}))

	expectedChild12 := expectedTx.writeNode(expectedTx.newNode(createItems("i", "j"), []pgnum{}))

	expectedChild1 := expectedTx.writeNode(expectedTx.newNode(createItems("b", "h"), []pgnum{expectedChild10.pageNum, expectedChild11.pageNum, expectedChild12.pageNum}))

	expectedRoot := expectedTx.writeNode(expectedTx.newNode(createItems("8"), []pgnum{expectedChild0.pageNum, expectedChild1.pageNum}))

	expectedCollection, err := expectedTx.createCollection(newCollection(testCollectionName, expectedRoot.pageNum))
	require.NoError(t, err)

	err = expectedTx.Commit()
	require.NoError(t, err)

	// Remove an element
	areTreesEqual(t, expectedCollection, collection)
}

func Test_RemoveFromInnerNodeAndUnion(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child00 := tx.writeNode(tx.newNode(createItems("0", "1"), []pgnum{}))

	child01 := tx.writeNode(tx.newNode(createItems("3", "4"), []pgnum{}))

	child02 := tx.writeNode(tx.newNode(createItems("6", "7"), []pgnum{}))

	child0 := tx.writeNode(tx.newNode(createItems("2", "5"), []pgnum{child00.pageNum, child01.pageNum, child02.pageNum}))

	child10 := tx.writeNode(tx.newNode(createItems("9", "a"), []pgnum{}))

	child11 := tx.writeNode(tx.newNode(createItems("c", "d"), []pgnum{}))

	child12 := tx.writeNode(tx.newNode(createItems("f", "g"), []pgnum{}))

	child1 := tx.writeNode(tx.newNode(createItems("b", "e"), []pgnum{child10.pageNum, child11.pageNum, child12.pageNum}))

	root := tx.writeNode(tx.newNode(createItems("8"), []pgnum{child0.pageNum, child1.pageNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pageNum))
	require.NoError(t, err)

	// Remove an element
	err = collection.Remove(createItem("2"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTx := expectedDB.WriteTx()

	expectedChild0 := expectedTx.writeNode(expectedTx.newNode(createItems("0", "1", "3", "4"), []pgnum{}))

	expectedChild1 := expectedTx.writeNode(expectedTx.newNode(createItems("6", "7"), []pgnum{}))

	expectedChild2 := expectedTx.writeNode(expectedTx.newNode(createItems("9", "a"), []pgnum{}))

	expectedChild3 := expectedTx.writeNode(expectedTx.newNode(createItems("c", "d"), []pgnum{}))

	expectedChild4 := expectedTx.writeNode(expectedTx.newNode(createItems("f", "g"), []pgnum{}))

	expectedRoot := expectedTx.writeNode(expectedTx.newNode(createItems("5", "8", "b", "e"), []pgnum{expectedChild0.pageNum, expectedChild1.pageNum, expectedChild2.pageNum, expectedChild3.pageNum, expectedChild4.pageNum}))

	expectedCollection, err := expectedTx.createCollection(newCollection(testCollectionName, expectedRoot.pageNum))
	require.NoError(t, err)

	err = expectedTx.Commit()
	require.NoError(t, err)

	// Remove an element
	areTreesEqual(t, expectedCollection, collection)
}

func Test_RemoveFromLeafAndRotateLeft(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child00 := tx.writeNode(tx.newNode(createItems("0", "1"), []pgnum{}))

	child01 := tx.writeNode(tx.newNode(createItems("3", "4", "5"), []pgnum{}))

	child02 := tx.writeNode(tx.newNode(createItems("7", "8"), []pgnum{}))

	child0 := tx.writeNode(tx.newNode(createItems("2", "6"), []pgnum{child00.pageNum, child01.pageNum, child02.pageNum}))

	child10 := tx.writeNode(tx.newNode(createItems("a", "b"), []pgnum{}))

	child11 := tx.writeNode(tx.newNode(createItems("d", "e"), []pgnum{}))

	child12 := tx.writeNode(tx.newNode(createItems("g", "h"), []pgnum{}))

	child1 := tx.writeNode(tx.newNode(createItems("c", "f"), []pgnum{child10.pageNum, child11.pageNum, child12.pageNum}))

	root := tx.writeNode(tx.newNode(createItems("9"), []pgnum{child0.pageNum, child1.pageNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pageNum))
	require.NoError(t, err)

	// Remove an element
	err = collection.Remove(createItem("1"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTx := expectedDB.WriteTx()

	expectedChild00 := expectedTx.writeNode(expectedTx.newNode(createItems("0", "2"), []pgnum{}))

	expectedChild01 := expectedTx.writeNode(expectedTx.newNode(createItems("4", "5"), []pgnum{}))

	expectedChild02 := expectedTx.writeNode(expectedTx.newNode(createItems("7", "8"), []pgnum{}))

	expectedChild0 := expectedTx.writeNode(expectedTx.newNode(createItems("3", "6"), []pgnum{expectedChild00.pageNum, expectedChild01.pageNum, expectedChild02.pageNum}))

	expectedChild10 := expectedTx.writeNode(expectedTx.newNode(createItems("a", "b"), []pgnum{}))

	expectedChild11 := expectedTx.writeNode(expectedTx.newNode(createItems("d", "e"), []pgnum{}))

	expectedChild12 := expectedTx.writeNode(expectedTx.newNode(createItems("g", "h"), []pgnum{}))

	expectedChild1 := expectedTx.writeNode(expectedTx.newNode(createItems("c", "f"), []pgnum{expectedChild10.pageNum, expectedChild11.pageNum, expectedChild12.pageNum}))

	expectedRoot := expectedTx.writeNode(expectedTx.newNode(createItems("9"), []pgnum{expectedChild0.pageNum, expectedChild1.pageNum}))

	expectedCollection, err := expectedTx.createCollection(newCollection(testCollectionName, expectedRoot.pageNum))
	require.NoError(t, err)

	err = expectedTx.Commit()
	require.NoError(t, err)

	// Remove an element
	areTreesEqual(t, expectedCollection, collection)
}

func Test_RemoveFromLeafAndRotateRight(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child00 := tx.writeNode(tx.newNode(createItems("0", "1"), []pgnum{}))

	child01 := tx.writeNode(tx.newNode(createItems("3", "4", "5"), []pgnum{}))

	child02 := tx.writeNode(tx.newNode(createItems("7", "8"), []pgnum{}))

	child0 := tx.writeNode(tx.newNode(createItems("2", "6"), []pgnum{child00.pageNum, child01.pageNum, child02.pageNum}))

	child10 := tx.writeNode(tx.newNode(createItems("a", "b"), []pgnum{}))

	child11 := tx.writeNode(tx.newNode(createItems("d", "e"), []pgnum{}))

	child12 := tx.writeNode(tx.newNode(createItems("g", "h"), []pgnum{}))

	child1 := tx.writeNode(tx.newNode(createItems("c", "f"), []pgnum{child10.pageNum, child11.pageNum, child12.pageNum}))

	root := tx.writeNode(tx.newNode(createItems("9"), []pgnum{child0.pageNum, child1.pageNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pageNum))
	require.NoError(t, err)

	// Remove an element
	err = collection.Remove(createItem("8"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTx := expectedDB.WriteTx()

	expectedChild00 := expectedTx.writeNode(expectedTx.newNode(createItems("0", "1"), []pgnum{}))

	expectedChild01 := expectedTx.writeNode(expectedTx.newNode(createItems("3", "4"), []pgnum{}))

	expectedChild02 := expectedTx.writeNode(expectedTx.newNode(createItems("6", "7"), []pgnum{}))

	expectedChild0 := expectedTx.writeNode(expectedTx.newNode(createItems("2", "5"), []pgnum{expectedChild00.pageNum, expectedChild01.pageNum, expectedChild02.pageNum}))

	expectedChild10 := expectedTx.writeNode(expectedTx.newNode(createItems("a", "b"), []pgnum{}))

	expectedChild11 := expectedTx.writeNode(expectedTx.newNode(createItems("d", "e"), []pgnum{}))

	expectedChild12 := expectedTx.writeNode(expectedTx.newNode(createItems("g", "h"), []pgnum{}))

	expectedChild1 := expectedTx.writeNode(expectedTx.newNode(createItems("c", "f"), []pgnum{expectedChild10.pageNum, expectedChild11.pageNum, expectedChild12.pageNum}))

	expectedRoot := expectedTx.writeNode(expectedTx.newNode(createItems("9"), []pgnum{expectedChild0.pageNum, expectedChild1.pageNum}))

	expectedCollection, err := expectedTx.createCollection(newCollection(testCollectionName, expectedRoot.pageNum))
	require.NoError(t, err)

	err = expectedTx.Commit()
	require.NoError(t, err)

	// Remove an element
	areTreesEqual(t, expectedCollection, collection)
}

func Test_RemoveFromLeafAndUnion(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child00 := tx.writeNode(tx.newNode(createItems("0", "1"), []pgnum{}))

	child01 := tx.writeNode(tx.newNode(createItems("3", "4"), []pgnum{}))

	child02 := tx.writeNode(tx.newNode(createItems("6", "7"), []pgnum{}))

	child0 := tx.writeNode(tx.newNode(createItems("2", "5"), []pgnum{child00.pageNum, child01.pageNum, child02.pageNum}))

	child10 := tx.writeNode(tx.newNode(createItems("9", "a"), []pgnum{}))

	child11 := tx.writeNode(tx.newNode(createItems("c", "d"), []pgnum{}))

	child12 := tx.writeNode(tx.newNode(createItems("f", "g"), []pgnum{}))

	child1 := tx.writeNode(tx.newNode(createItems("b", "e"), []pgnum{child10.pageNum, child11.pageNum, child12.pageNum}))

	root := tx.writeNode(tx.newNode(createItems("8"), []pgnum{child0.pageNum, child1.pageNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pageNum))
	require.NoError(t, err)

	// Remove an element
	err = collection.Remove(createItem("0"))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expectedDB, expectedCleanFunc := createTestDB(t)
	defer expectedCleanFunc()

	expectedTx := expectedDB.WriteTx()

	expectedChild0 := expectedTx.writeNode(expectedTx.newNode(createItems("1", "2", "3", "4"), []pgnum{}))

	expectedChild1 := expectedTx.writeNode(expectedTx.newNode(createItems("6", "7"), []pgnum{}))

	expectedChild2 := expectedTx.writeNode(expectedTx.newNode(createItems("9", "a"), []pgnum{}))

	expectedChild3 := expectedTx.writeNode(expectedTx.newNode(createItems("c", "d"), []pgnum{}))

	expectedChild4 := expectedTx.writeNode(expectedTx.newNode(createItems("f", "g"), []pgnum{}))

	expectedRoot := expectedTx.writeNode(expectedTx.newNode(createItems("5", "8", "b", "e"), []pgnum{expectedChild0.pageNum, expectedChild1.pageNum, expectedChild2.pageNum, expectedChild3.pageNum, expectedChild4.pageNum}))

	expectedCollection, err := expectedTx.createCollection(newCollection(testCollectionName, expectedRoot.pageNum))
	require.NoError(t, err)

	err = expectedTx.Commit()
	require.NoError(t, err)

	// Remove an element
	areTreesEqual(t, expectedCollection, collection)
}

func Test_FindNode(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child00 := tx.writeNode(tx.newNode(createItems("0", "1"), []pgnum{}))

	child01 := tx.writeNode(tx.newNode(createItems("3", "4"), []pgnum{}))

	child02 := tx.writeNode(tx.newNode(createItems("6", "7"), []pgnum{}))

	child0 := tx.writeNode(tx.newNode(createItems("2", "5"), []pgnum{child00.pageNum, child01.pageNum, child02.pageNum}))

	child10 := tx.writeNode(tx.newNode(createItems("9", "a"), []pgnum{}))

	child11 := tx.writeNode(tx.newNode(createItems("c", "d"), []pgnum{}))

	child12 := tx.writeNode(tx.newNode(createItems("f", "g"), []pgnum{}))

	child1 := tx.writeNode(tx.newNode(createItems("b", "e"), []pgnum{child10.pageNum, child11.pageNum, child12.pageNum}))

	root := tx.writeNode(tx.newNode(createItems("8"), []pgnum{child0.pageNum, child1.pageNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pageNum))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	// Item found
	expectedVal := createItem("c")
	expectedItem := newItem(expectedVal, expectedVal)
	item, err := collection.Find(expectedVal)
	require.NoError(t, err)
	assert.Equal(t, expectedItem, item)

	// Item not found
	expectedVal = createItem("h")
	item, err = collection.Find(expectedVal)
	require.NoError(t, err)
	assert.Nil(t, item)
}

func Test_UpdateNode(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	child00 := tx.writeNode(tx.newNode(createItems("0", "1"), []pgnum{}))

	child01 := tx.writeNode(tx.newNode(createItems("3", "4"), []pgnum{}))

	child02 := tx.writeNode(tx.newNode(createItems("6", "7"), []pgnum{}))

	child0 := tx.writeNode(tx.newNode(createItems("2", "5"), []pgnum{child00.pageNum, child01.pageNum, child02.pageNum}))

	child10 := tx.writeNode(tx.newNode(createItems("9", "a"), []pgnum{}))

	child11 := tx.writeNode(tx.newNode(createItems("c", "d"), []pgnum{}))

	child12 := tx.writeNode(tx.newNode(createItems("f", "g"), []pgnum{}))

	child1 := tx.writeNode(tx.newNode(createItems("b", "e"), []pgnum{child10.pageNum, child11.pageNum, child12.pageNum}))

	root := tx.writeNode(tx.newNode(createItems("8"), []pgnum{child0.pageNum, child1.pageNum}))

	collection, err := tx.createCollection(newCollection(testCollectionName, root.pageNum))
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	tx2 := db.WriteTx()
	collection, err = tx2.GetCollection(collection.name)
	require.NoError(t, err)

	// Item found
	expectedVal := createItem("c")
	expectedItem := newItem(expectedVal, expectedVal)
	item, err := collection.Find(expectedVal)
	require.NoError(t, err)
	assert.Equal(t, expectedItem, item)

	// Item updated successfully
	newvalue := createItem("f")
	err = collection.Put(expectedVal, newvalue)
	require.NoError(t, err)

	item, err = collection.Find(expectedVal)
	require.NoError(t, err)
	assert.Equal(t, newvalue, item.value)

	err = tx2.Commit()
	require.NoError(t, err)
}

func TestSerializeWithoutChildNodes(t *testing.T) {
	items := []*Item{newItem([]byte("key1"), []byte("val1")), newItem([]byte("key2"), []byte("val2"))}
	var childNodes []pgnum
	node := &Node{
		items:      items,
		childNodes: childNodes,
	}

	actual := node.serialize(testPageSize)

	expectedPage, err := os.ReadFile(getExpectedResultFileName(t.Name()))
	require.NoError(t, err)
	assert.Equal(t, 0, bytes.Compare(actual, expectedPage))
}

func TestDeserializeWithoutChildNodes(t *testing.T) {
	page, err := os.ReadFile(getExpectedResultFileName(t.Name()))
	require.NoError(t, err)

	actualNode := NewEmptyNode()
	actualNode.deserialize(page)

	items := []*Item{newItem([]byte("key1"), []byte("val1")), newItem([]byte("key2"), []byte("val2"))}
	var childNodes []pgnum
	expectedNode := &Node{
		items:      items,
		childNodes: childNodes,
	}

	assert.Equal(t, expectedNode, actualNode)
}

func TestSerializeWithChildNodes(t *testing.T) {
	items := []*Item{newItem([]byte("key1"), []byte("val1")), newItem([]byte("key2"), []byte("val2"))}
	childNodes := []pgnum{1, 2, 3}
	node := &Node{
		items:      items,
		childNodes: childNodes,
	}

	actual := node.serialize(testPageSize)

	expectedPage, err := os.ReadFile(getExpectedResultFileName(t.Name()))
	require.NoError(t, err)
	assert.Equal(t, 0, bytes.Compare(actual, expectedPage))
}

func TestDeserializeWithChildNodes(t *testing.T) {
	page, err := os.ReadFile(getExpectedResultFileName(t.Name()))
	require.NoError(t, err)

	items := []*Item{newItem([]byte("key1"), []byte("val1")), newItem([]byte("key2"), []byte("val2"))}
	childNodes := []pgnum{1, 2, 3}
	expectedNode := &Node{
		items:      items,
		childNodes: childNodes,
	}

	actualNode := NewEmptyNode()
	actualNode.deserialize(page)
	assert.Equal(t, expectedNode, actualNode)
}
