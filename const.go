package LibraDB

import "errors"

const (
	magicNumberSize = 4
	counterSize = 4
	nodeHeaderSize = 3

	collectionSize = 16
	pageNumSize    = 8
	metaSize       = 20
)

var writeInsideReadTxErr = errors.New("can't perform a write operation inside a read transaction")