package LibraDB

import "encoding/binary"

// metaPage is the maximum pgnum that is used by the db for its own purposes. For now, only page 0 is used as the
// header page. It means all other page numbers can be used.
const metaPage = 0

// freelist manages the manages free and used pages.
type freelist struct {
	// maxPage holds the latest page num allocated. releasedPages holds all the ids that were released during
	// delete. New page ids are first given from the releasedPageIDs to avoid growing the file. If it's empty, then
	// maxPage is incremented and a new page is created thus increasing the file size.
	maxPage       pgnum
	releasedPages []pgnum
}

func newFreelist() *freelist {
	return &freelist{
		maxPage:       metaPage,
		releasedPages: []pgnum{},
	}
}

// getNextPage returns page ids for writing New page ids are first given from the releasedPageIDs to avoid growing
// the file. If it's empty, then maxPage is incremented and a new page is created thus increasing the file size.
func (fr *freelist) getNextPage() pgnum {
	if len(fr.releasedPages) != 0 {
		// Take the last element and remove it from the list
		pageID := fr.releasedPages[len(fr.releasedPages)-1]
		fr.releasedPages = fr.releasedPages[:len(fr.releasedPages)-1]
		return pageID
	}
	fr.maxPage += 1
	return fr.maxPage
}

func (fr *freelist) releasePage(page pgnum) {
	fr.releasedPages = append(fr.releasedPages, page)
}

func (fr *freelist) serialize(buf []byte) []byte {
	pos := 0

	binary.LittleEndian.PutUint16(buf[pos:], uint16(fr.maxPage))
	pos += 2

	// released pages count
	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(fr.releasedPages)))
	pos += 2

	for _, page := range fr.releasedPages {
		binary.LittleEndian.PutUint64(buf[pos:], uint64(page))
		pos += pageNumSize

	}
	return buf
}

func (fr *freelist) deserialize(buf []byte) {
	pos := 0
	fr.maxPage = pgnum(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2

	// released pages count
	releasedPagesCount := int(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2

	for i := 0; i < releasedPagesCount; i++ {
		fr.releasedPages = append(fr.releasedPages, pgnum(binary.LittleEndian.Uint64(buf[pos:])))
		pos += pageNumSize
	}
}
