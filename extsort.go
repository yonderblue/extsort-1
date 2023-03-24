package extsort

import (
	"io"
)

// Sorter is responsible for sorting.
type Sorter struct {
	opt *Options
	buf *memBuffer
	tw  *tempWriter
}

// New inits a sorter
func New(opt *Options) *Sorter {
	opt = opt.norm()
	return &Sorter{opt: opt, buf: &memBuffer{compare: opt.Compare}}
}

// Append appends a data chunk to the sorter.
func (s *Sorter) Append(data []byte) error {
	return s.Put(data, nil)
}

// Put inserts a key value pair into the sorter.
func (s *Sorter) Put(key, value []byte) error {
	if sz := s.buf.ByteSize(); sz > 0 && sz+len(key)+len(value) > s.opt.BufferSize {
		if err := s.flush(); err != nil {
			return err
		}
	}

	s.buf.Append(key, value)
	return nil
}

// Sort applies the sort algorithm and returns an interator.
func (s *Sorter) Sort() (*Iterator, error) {
	if err := s.flush(); err != nil {
		return nil, err
	}

	// free the write buffer
	s.buf.Free()

	// wrap in an iterator
	return newIterator(s.tw.ReaderAt(), s.tw.offsets, s.opt)
}

// Close stops the processing and removes temporary files.
func (s *Sorter) Close() error {
	if s.tw != nil {
		return s.tw.Close()
	}
	return nil
}

// Size returns the buffered and written size.
func (s *Sorter) Size() int64 {
	sum := int64(s.buf.ByteSize())
	if s.tw == nil {
		return sum
	}
	return sum + s.tw.Size()
}

func (s *Sorter) flush() error {
	if s.tw == nil {
		tw, err := newTempWriter(s.opt.WorkDir, s.opt.Compression, s.opt.KeepFiles)
		if err != nil {
			return err
		}
		s.tw = tw
	}

	dedup := s.opt.dedup()

	s.opt.Sort(s.buf)

	for i := 0; i < len(s.buf.ents); i++ {
		ent := s.buf.ents[i]

		if i+1 < len(s.buf.ents) {
			next := s.buf.ents[i+1]

			if dedup(ent.Key(), next.Key()) {
				continue
			}
		}

		if err := s.tw.Encode(ent.entry); err != nil {
			return err
		}
	}
	if err := s.tw.Flush(); err != nil {
		return err
	}

	s.buf.Reset()
	return nil
}

// --------------------------------------------------------------------

// Iterator instances are used to iterate over sorted output.
type Iterator struct {
	tr   *tempReader
	heap *minHeap

	ent     *entry
	nextEnt *entry
	dedupe  Equal
	err     error
	nextOK  bool
}

func newIterator(ra io.ReaderAt, offsets []int64, opt *Options) (*Iterator, error) {
	tr, err := newTempReader(ra, offsets, opt.BufferSize, opt.Compression)
	if err != nil {
		return nil, err
	}

	iter := &Iterator{tr: tr, heap: &minHeap{compare: opt.Compare}, dedupe: opt.dedup(), ent: fetchEntry()}
	for i := 0; i < tr.NumSections(); i++ {
		if err := iter.fillHeap(i); err != nil {
			_ = tr.Close()
			return nil, err
		}
	}

	iter.nextOK = iter.next()

	return iter, nil
}

// Next advances the iterator to the next item and returns true if successful.
func (i *Iterator) Next() bool {
	if !i.nextOK {
		return false
	}

	copyEntry(i.ent, i.nextEnt)

	for i.next() {
		if i.dedupe(i.ent.Key(), i.nextEnt.Key()) {
			copyEntry(i.ent, i.nextEnt)
			continue
		}
		return true
	}
	i.nextOK = false
	return true
}

func (i *Iterator) next() bool {
	if i.err != nil {
		return false
	}
	if i.heap.Len() == 0 {
		return false
	}

	section, ent := i.heap.PopEntry()
	if err := i.fillHeap(section); err != nil {
		ent.Release()
		i.err = err
		return false
	}

	prev := i.nextEnt
	i.nextEnt = ent
	if prev != nil {
		prev.Release()
	}
	return true
}

// Key returns the key at the current cursor position.
func (i *Iterator) Key() []byte {
	return i.ent.Key()
}

// Value returns the value at the current cursor position.
func (i *Iterator) Value() []byte {
	return i.ent.Val()
}

// Data returns the data at the current cursor position (alias for Key).
func (i *Iterator) Data() []byte {
	return i.ent.Key()
}

// Err returns the error, if occurred.
func (i *Iterator) Err() error {
	return i.err
}

// Close closes the iterator.
func (i *Iterator) Close() error {
	if i.nextEnt != nil {
		i.nextEnt.Release()
		i.nextEnt = nil
	}
	if i.ent != nil {
		i.ent.Release()
		i.ent = nil
	}

	return i.tr.Close()
}

func (i *Iterator) fillHeap(section int) error {
	ent, err := i.tr.ReadNext(section)
	if err != nil {
		return err
	}
	if ent != nil {
		i.heap.PushEntry(section, ent)
	}
	return nil
}
