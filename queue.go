package elastilog

import (
	"sync"
	"time"
)

type FlushFunc func([]Entry)

type Queue interface {
	Flush()
	Add(...Entry)
	Close()
}

type queue struct {
	interval time.Duration
	flusher  FlushFunc
	buffer   []Entry
	inner    chan Entry
	wg       sync.WaitGroup
}

func NewQueue(flusher FlushFunc, capacity uint, interval time.Duration) Queue {
	q := &queue{
		interval: interval,
		flusher:  flusher,
		buffer:   make([]Entry, 0, capacity+1),
		inner:    make(chan Entry, capacity),
	}
	go q.open()
	return q
}

func (q *queue) Flush() {
	if len(q.buffer) == 0 {
		return
	}
	q.wg.Add(1)
	tmp := make([]Entry, len(q.buffer))
	copy(tmp, q.buffer)
	q.buffer = make([]Entry, 0, cap(q.buffer))

	go func() {
		q.flusher(tmp)
		q.wg.Done()
	}()
}

func (q *queue) Add(msgs ...Entry) {
	for _, msg := range msgs {
		q.inner <- msg
	}
}

func (q *queue) open() {
	count := 0
	for {
		timeout := time.After(q.interval)
		select {
		case msg, ok := <-q.inner:
			if !ok {
				q.Flush()
				return
			}
			q.buffer = append(q.buffer, msg)
			count++
		case <-timeout:
			q.Flush()
		}
		//Flush when close to full
		if cap(q.buffer)-1 <= len(q.buffer) {
			q.Flush()
		}
	}
}

func (q *queue) Close() {
	close(q.inner)
	q.wg.Wait()
}
