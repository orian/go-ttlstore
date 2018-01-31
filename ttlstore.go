package ttlstore

import (
	"sync"
	"time"
)

type SetCallback func(key interface{}, insert time.Time)

// TtlStore is a map store with callback on set. It adds `Delete` method
// used by Deleter.
type TtlStore struct {
	m           *sync.RWMutex
	store       map[interface{}]interface{}
	setCallback SetCallback
}

func New(c SetCallback) *TtlStore {
	return &TtlStore{
		&sync.RWMutex{},
		make(map[interface{}]interface{}),
		c,
	}
}

func (t *TtlStore) Has(key interface{}) bool {
	t.m.RLock()
	_, ok := t.store[key]
	t.m.RUnlock()
	return ok
}

func (t *TtlStore) GetHas(key interface{}) (interface{}, bool) {
	t.m.RLock()
	v, ok := t.store[key]
	t.m.RUnlock()
	return v, ok
}

func (t *TtlStore) Get(key interface{}) interface{} {
	t.m.RLock()
	defer t.m.RUnlock()
	return t.store[key]
}

func (t *TtlStore) Set(key, value interface{}) {
	t.m.Lock()
	t.store[key] = value
	t.m.Unlock()
	if t.setCallback != nil {
		t.setCallback(key, time.Now())
	}
}

func (t *TtlStore) Delete(key interface{}) {
	t.m.Lock()
	// NOTICE: we ignore the callback. It means, if a Delete is used
	// by developer, the deleter must be notified in separate.
	delete(t.store, key)
	t.m.Unlock()
}
