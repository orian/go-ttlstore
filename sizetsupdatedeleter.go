package ttlstore

import (
	"github.com/sirupsen/logrus"
	"github.com/orian/utils/ptime"

	"container/heap"
	"io/ioutil"
	"sync"
	"time"
)

type itemTimestamp struct {
	Key           interface{}
	TimestmapNsec int64
}

type itemTimestmapHeap []*itemTimestamp

func (h itemTimestmapHeap) Len() int { return len(h) }
func (h itemTimestmapHeap) Less(i, j int) bool {
	return h[i].TimestmapNsec > h[j].TimestmapNsec
}
func (h itemTimestmapHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *itemTimestmapHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*itemTimestamp))
}

func (h *itemTimestmapHeap) Pop() interface{} {
	old := *h
	n := len(old) - 1
	x := old[n]
	old[n] = nil
	*h = old[0:n]
	return x
}

type DeleteHook func(key interface{})

// Deleter implements a clean up mechanism.
// Deleter keeps at least `keep` items. The items must be younger then `maxAge`.
// One should call Set method for keys one wants to keep for a specific time.
// The deletion process is triggered by a timer set to `Interval` value.
// It removes items from the oldest to the newest until we have less than `keep`
// items.
type Deleter struct {
	c          chan *itemTimestamp
	DeleteFunc DeleteHook
	Interval   time.Duration
	Name       string

	lastItemTimeNsec map[interface{}]int64
	maxAge           time.Duration
	keepNum          int

	m        *sync.Mutex
	end      chan bool
	finished chan bool
	logger   logrus.FieldLogger
}

func NewDeleter(name string, del DeleteHook, maxAge, runInterval time.Duration, bufSize, keep int,
	logger logrus.FieldLogger) *Deleter {

	if logger == nil {
		l := logrus.New()
		l.Out = ioutil.Discard
		logger = l
	}

	return &Deleter{
		make(chan *itemTimestamp, bufSize),
		del,
		runInterval,
		name,
		make(map[interface{}]int64),
		maxAge,
		keep,
		&sync.Mutex{},
		make(chan bool, 1),
		make(chan bool, 2),
		logger,
	}
}

func (d *Deleter) Set(key interface{}, insert time.Time) {
	// TODO channel may be overengineering. Switch to sequential.
	d.c <- &itemTimestamp{key, insert.UnixNano()}
}

type DeleterProvider func(name string, hook DeleteHook) *Deleter

func NewDeleterProvider(maxAge, interval time.Duration, bufSize, keep int) DeleterProvider {
	return func(name string, del DeleteHook) *Deleter {
		return NewDeleter(name, del, maxAge, interval, bufSize, keep, nil)
	}
}

func (d *Deleter) deleteTooOld() {
	num := 0
	thresholdUsec := time.Now().Add(-d.maxAge).UnixNano()
	t := ptime.NewTimer()
	d.m.Lock()
	defer d.m.Unlock()
	for k, v := range d.lastItemTimeNsec {
		if v < thresholdUsec {
			if d.DeleteFunc != nil {
				d.DeleteFunc(k)
			}
			delete(d.lastItemTimeNsec, k)
			num++
		}
	}
	d.logger.Infof("[%s] deleted %d too old (%s) items, kept %d, took: %s",
		d.Name, num, d.maxAge, len(d.lastItemTimeNsec), t.Duration())
}

func (d *Deleter) deleteUntilBelowKeepNum() {
	d.m.Lock()
	defer d.m.Unlock()
	l := len(d.lastItemTimeNsec)
	if l < int(float32(d.keepNum)*1.1) {
		return
	}

	t := ptime.NewTimer()
	num := 0
	h := &itemTimestmapHeap{}
	numToRemove := len(d.lastItemTimeNsec) - d.keepNum
	initDone := false
	for k, v := range d.lastItemTimeNsec {
		heap.Push(h, &itemTimestamp{k, v})
		if initDone {
			heap.Pop(h)
		} else if h.Len() >= numToRemove {
			heap.Init(h)
			initDone = true
		}
	}

	for i := h.Len(); i > 0; i-- {
		a := heap.Pop(h).(*itemTimestamp)
		if d.DeleteFunc != nil {
			d.DeleteFunc(a.Key)
		}
		delete(d.lastItemTimeNsec, a.Key)
		num++
	}
	d.logger.Infof("[%s] deleted %d items because too many, kept: %d, took: %s",
		d.Name, num, len(d.lastItemTimeNsec), t.Duration())
}

func (d *Deleter) Process() {
	d.logger.Infof("[%s] process started", d.Name)
	for t := range d.c {
		d.m.Lock()
		if v := d.lastItemTimeNsec[t.Key]; v < t.TimestmapNsec {
			d.lastItemTimeNsec[t.Key] = t.TimestmapNsec
		}
		d.m.Unlock()
	}
	d.finished <- true
	d.logger.Infof("[%s] process finished", d.Name)
}

func (d *Deleter) Deleting() {
	d.logger.Infof("[%s] deleting started", d.Name)
	tmr := time.NewTicker(d.Interval)
	for {
		select {
		case <-d.end:
			goto end
		case <-tmr.C:
			d.deleteTooOld()
			d.deleteUntilBelowKeepNum()
		}
	}
end:
	tmr.Stop()
	d.finished <- true
	d.logger.Infof("[%s] deleting finished", d.Name)
}

func (d *Deleter) Start() {
	go d.Deleting()
	d.Process()
}

func (d *Deleter) Stop() {
	close(d.c)
	d.end <- true
	<-d.finished
	<-d.finished
}
