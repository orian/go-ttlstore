package ttlstore

import (
	"sync"
	"time"
)

type SetCallback func(key interface{}, insert time.Time)

// TtlStore is a map store with callback on Set. It wraps around the sync.Map.
// It adds `Delete` method used by Deleter.
type TtlStore struct {
	sm          sync.Map
	setCallback SetCallback

	Now func() time.Time
}

func New(c SetCallback) *TtlStore {
	return &TtlStore{
		sync.Map{},
		c,
		time.Now,
	}
}

func (t *TtlStore) Has(key interface{}) bool {
	_, ok := t.sm.Load(key)
	return ok
}

func (t *TtlStore) GetHas(key interface{}) (interface{}, bool) {
	v, ok := t.sm.Load(key)
	return v, ok
}

func (t *TtlStore) Get(key interface{}) interface{} {
	v, _ := t.sm.Load(key)
	return v
}

func (t *TtlStore) Set(key, value interface{}) {
	t.sm.Store(key, value)
	if t.setCallback != nil {
		t.setCallback(key, t.Now())
	}
}

func (t *TtlStore) Delete(key interface{}) {
	t.sm.Delete(key)
}
