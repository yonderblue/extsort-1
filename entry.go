package extsort

import (
	"github.com/valyala/bytebufferpool"
)

var entryPool bytebufferpool.Pool

type entry struct {
	data   *bytebufferpool.ByteBuffer
	keyLen int
}

func fetchEntry() *entry {
	return &entry{entryPool.Get(), 0}
}

func copyEntry(dst, src *entry) {
	dst.data.B = append(dst.data.B[:0], src.data.B...)
	dst.keyLen = src.keyLen
}

func (e entry) Key() []byte {
	return e.data.B[:e.keyLen]
}

func (e entry) Val() []byte {
	return e.data.B[e.keyLen:]
}

func (e entry) ValLen() int {
	return len(e.data.B) - e.keyLen
}

func (e *entry) Release() {
	entryPool.Put(e.data)
}
