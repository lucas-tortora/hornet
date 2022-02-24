package dag

import (
	"container/list"
	"sync"
)

// SyncList represents a thread safe doubly linked list.
// The zero value for List is an empty list ready to use.
type SyncList struct {
	sync.RWMutex

	// stack holding the ordered msg to process
	stack *list.List
}

// NewSyncList returns an initialized thread safe list.
func NewSyncList() *SyncList {
	return &SyncList{stack: list.New()}
}

// Len returns the number of elements of list l.
// The complexity is O(1).
func (tl *SyncList) Len() int {
	tl.RLock()
	defer tl.RUnlock()

	return tl.stack.Len()
}

// PushFront inserts a new element e with value v at the front of list l and returns e.
func (tl *SyncList) PushFront(v interface{}) *list.Element {
	tl.Lock()
	defer tl.Unlock()

	return tl.stack.PushFront(v)
}

// Front returns the first element of list l or nil if the list is empty.
func (tl *SyncList) Front() *list.Element {
	tl.RLock()
	defer tl.RUnlock()

	return tl.stack.Front()
}

// Remove removes e from l if e is an element of list l.
// It returns the element value e.Value.
// The element must not be nil.
func (tl *SyncList) Remove(e *list.Element) interface{} {
	tl.Lock()
	defer tl.Unlock()

	return tl.stack.Remove(e)
}
