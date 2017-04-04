package ttlstore

import (
	"github.com/Sirupsen/logrus"
	"github.com/orian/utils/ptime"

	"container/heap"
	"io/ioutil"
	"sync"
	"time"
)

type actionTimestamp struct {
	Key           interface{}
	TimestmapNsec int64
}

type ActionTimestmapHeap []*actionTimestamp

func (h ActionTimestmapHeap) Len() int { return len(h) }
func (h ActionTimestmapHeap) Less(i, j int) bool {
	return h[i].TimestmapNsec > h[j].TimestmapNsec
}
func (h ActionTimestmapHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *ActionTimestmapHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*actionTimestamp))
}

func (h *ActionTimestmapHeap) Pop() interface{} {
	old := *h
	n := len(old) - 1
	x := old[n]
	old[n] = nil
	*h = old[0:n]
	return x
}

type DeleteHook func(key interface{})

type Deleter struct {
	c          chan *actionTimestamp
	DeleteFunc DeleteHook
	Interval   time.Duration
	Name       string

	lastActionTimestamp map[interface{}]int64
	maxAge              time.Duration
	keepNum             int

	m        *sync.Mutex
	end      chan bool
	finished chan bool
	logger   logrus.FieldLogger
}

var _ Janitor = &Deleter{}

func NewDeleter(name string, del DeleteHook, maxAge, runInterval time.Duration, bufSize, keep int,
	logger logrus.FieldLogger) *Deleter {

	if logger == nil {
		logger = logrus.New()
		logger.Out = ioutil.Discard
	}

	return &Deleter{
		make(chan *actionTimestamp, bufSize),
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
	d.c <- &actionTimestamp{key, insert.UnixNano()}
}

type DeleterProvider func(name string, hook DeleteHook) *Deleter

func NewDeleterProvider(maxAge, interval time.Duration, bufSize, keep int) DeleterProvider {
	return func(name string, del DeleteHook) *Deleter {
		return NewDeleter(name, del, maxAge, interval, bufSize, keep)
	}
}

func (d *Deleter) DeleteTooOld() {
	num := 0
	thresholdUsec := time.Now().Add(-d.maxAge).UnixNano()
	t := ptime.NewTimer()
	d.m.Lock()
	defer d.m.Unlock()
	for k, v := range d.lastActionTimestamp {
		if v < thresholdUsec {
			if d.DeleteFunc != nil {
				d.DeleteFunc(k)
			}
			delete(d.lastActionTimestamp, k)
			num++
		}
	}
	d.logger.Infof("[%s] deleted %d too old (%s) items, kept %d, took: %s",
		d.Name, num, d.maxAge, len(d.lastActionTimestamp), t.Duration())
}

func (d *Deleter) deleteUntilBelowMaxUuid() {
	d.m.Lock()
	defer d.m.Unlock()
	l := len(d.lastActionTimestamp)
	if l < int(float32(d.keepNum)*1.1) {
		return
	}

	t := ptime.NewTimer()
	num := 0
	h := &ActionTimestmapHeap{}
	numToRemove := len(d.lastActionTimestamp) - d.keepNum
	initDone := false
	for k, v := range d.lastActionTimestamp {
		heap.Push(h, &actionTimestamp{k, v})
		if initDone {
			heap.Pop(h)
		} else if h.Len() >= numToRemove {
			heap.Init(h)
			initDone = true
		}
	}

	for i := h.Len(); i > 0; i-- {
		a := heap.Pop(h).(*actionTimestamp)
		if d.DeleteFunc != nil {
			d.DeleteFunc(a.Key)
		}
		delete(d.lastActionTimestamp, a.Key)
		num++
	}
	d.logger.Infof("[%s] deleted %d items because too many, kept: %d, took: %s",
		d.Name, num, len(d.lastActionTimestamp), t.Duration())
}

func (d *Deleter) Process() {
	d.logger.Infof("[%s] process started", d.Name)
	for t := range d.c {
		d.m.Lock()
		if v := d.lastActionTimestamp[t.Key]; v < t.TimestmapNsec {
			d.lastActionTimestamp[t.Key] = t.TimestmapNsec
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
			d.DeleteTooOld()
			d.deleteUntilBelowMaxUuid()
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
