package LibraDB

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestMetaSerialize(t *testing.T) {
	meta := newEmptyMeta()
	meta.root = 3
	meta.freelistPage = 4
	actual := meta.serialize()

	expected, err := os.ReadFile(getExpectedResultFileName(t.Name()))
	require.NoError(t, err)

	assert.Equal(t, expected, actual)
}

func TestCreateDalIncorrectMagicNumber(t *testing.T) {
	actualMetaBytes, err := os.ReadFile(getExpectedResultFileName(t.Name()))
	require.NoError(t, err)
	actualMeta := newEmptyMeta()
	assert.Panics(t, func() {
		actualMeta.deserialize(actualMetaBytes)
	})
}

func TestMetaDeserialize(t *testing.T) {
	actualMetaBytes, err := os.ReadFile(getExpectedResultFileName(t.Name()))
	actualMeta := newEmptyMeta()
	actualMeta.deserialize(actualMetaBytes)
	require.NoError(t, err)

	expectedMeta := newEmptyMeta()
	expectedMeta.root = 3
	expectedMeta.freelistPage = 4

	assert.Equal(t, expectedMeta, actualMeta)
}