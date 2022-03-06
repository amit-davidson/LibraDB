package main

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func Test_GetAndCreateCollection(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	collectionName := testCollectionName
	createdCollection, err := tx.CreateCollection(collectionName)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	tx = db.ReadTx()
	actual, err := tx.GetCollection(collectionName)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	expected := newEmptyCollection()
	expected.root = createdCollection.root
	expected.counter = 0
	expected.name = collectionName

	areCollectionsEqual(t, expected, actual)
}

func Test_GetCollectionDoesntExist(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.ReadTx()
	collection, err := tx.GetCollection([]byte("name1"))
	require.NoError(t, err)

	assert.Nil(t, collection)
}

func Test_CreateCollectionPutItem(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	collectionName := testCollectionName
	createdCollection, err := tx.CreateCollection(collectionName)
	require.NoError(t, err)

	newKey := []byte("0")
	newVal := []byte("1")
	err = createdCollection.Put(newKey, newVal)
	require.NoError(t, err)

	item, err := createdCollection.Find(newKey)
	require.NoError(t, err)

	assert.Equal(t, newKey, item.key)
	assert.Equal(t, newVal, item.value)
}

func Test_DeleteCollection(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	collectionName := testCollectionName
	createdCollection, err := tx.CreateCollection(collectionName)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	tx = db.WriteTx()
	actual, err := tx.GetCollection(collectionName)
	require.NoError(t, err)

	areCollectionsEqual(t, createdCollection, actual)

	err = tx.DeleteCollection(createdCollection.name)
	require.NoError(t, err)

	actualAfterRemoval, err := tx.GetCollection(collectionName)
	require.NoError(t, err)
	assert.Nil(t, actualAfterRemoval)

	err = tx.Commit()
	require.NoError(t, err)
}

func Test_DeleteItem(t *testing.T) {
	db, cleanFunc := createTestDB(t)
	defer cleanFunc()

	tx := db.WriteTx()

	collectionName := testCollectionName
	createdCollection, err := tx.CreateCollection(collectionName)
	require.NoError(t, err)

	newKey := []byte("0")
	newVal := []byte("1")
	err = createdCollection.Put(newKey, newVal)
	require.NoError(t, err)

	item, err := createdCollection.Find(newKey)
	require.NoError(t, err)

	assert.Equal(t, newKey, item.key)
	assert.Equal(t, newVal, item.value)

	err = createdCollection.Remove(item.key)
	require.NoError(t, err)

	item, err = createdCollection.Find(newKey)
	require.NoError(t, err)

	assert.Nil(t, item)
}
func TestSerializeCollection(t *testing.T) {
	expectedCollectionValue, err := os.ReadFile(getExpectedResultFileName(t.Name()))
	require.NoError(t, err)

	expected := &Item{
		key:   []byte("collection1"),
		value: expectedCollectionValue,
	}

	collection := &Collection{
		name:    []byte("collection1"),
		root:    1,
		counter: 1,
	}

	actual := collection.serialize()
	assert.Equal(t, expected, actual)
}

func TestDeserializeCollection(t *testing.T) {
	expectedCollectionValue, err := os.ReadFile(getExpectedResultFileName(t.Name()))

	expected := &Collection{
		name:    []byte("collection1"),
		root:    1,
		counter: 1,
	}

	collection := &Item{
		key:   []byte("collection1"),
		value: expectedCollectionValue,
	}
	actual := newEmptyCollection()
	actual.deserialize(collection)

	require.NoError(t, err)
	assert.Equal(t, expected, actual)
}
