package ttlstore

import (
	"sync"
	"time"
)

type Janitor interface {
	Set(key interface{}, insert time.Time)
}

type TtlStore struct {
	m     *sync.RWMutex
	store map[interface{}]interface{}
	j     Janitor
}

func New(j Janitor) *TtlStore {
	return &TtlStore{
		&sync.RWMutex{},
		make(map[interface{}]interface{}),
		j,
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
}

func (t *TtlStore) Delete(key interface{}) {
	t.m.Lock()
	delete(t.store, key)
	t.m.Unlock()
}
