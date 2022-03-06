package main

func main() {
	path := "libra.db"
	db, _ := Open(path, DefaultOptions)

	tx := db.WriteTx()
	name := []byte("test")
	collection, _ := tx.CreateCollection(name)

	key, value := []byte("key1"), []byte("value1")
	_ = collection.Put(key, value)

	_ = tx.Commit()
}