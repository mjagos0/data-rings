package eventlog

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type EventLog struct {
	ch		chan Event
	wg		sync.WaitGroup
	dropped		atomic.Uint64
	closed		atomic.Bool
	closeCh		chan struct{}
	nodeName	string
}

type Event struct {
	TS	time.Time	`json:"ts"`
	Node	string		`json:"node,omitempty"`
	Kind	string		`json:"kind"`
	Fields	map[string]any	`json:"f,omitempty"`
}

const channelCapacity = 65536

func New(path, nodeName string) (*EventLog, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("eventlog: mkdir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("eventlog: open %s: %w", path, err)
	}

	e := &EventLog{
		ch:		make(chan Event, channelCapacity),
		closeCh:	make(chan struct{}),
		nodeName:	nodeName,
	}
	e.wg.Add(1)
	go e.drain(f)
	return e, nil
}

func (e *EventLog) drain(f *os.File) {
	defer e.wg.Done()
	defer f.Close()
	w := bufio.NewWriterSize(f, 64*1024)
	enc := json.NewEncoder(w)
	flushTicker := time.NewTicker(200 * time.Millisecond)
	defer flushTicker.Stop()
	for {
		select {
		case ev, ok := <-e.ch:
			if !ok {
				_ = w.Flush()
				return
			}
			_ = enc.Encode(ev)
		case <-flushTicker.C:
			_ = w.Flush()
		}
	}
}

func (e *EventLog) Emit(kind string, fields map[string]any) {
	if e == nil || e.closed.Load() {
		return
	}
	ev := Event{
		TS:	time.Now().UTC(),
		Node:	e.nodeName,
		Kind:	kind,
		Fields:	fields,
	}
	select {
	case e.ch <- ev:
	default:
		e.dropped.Add(1)
	}
}

func (e *EventLog) Close() error {
	if e == nil || !e.closed.CompareAndSwap(false, true) {
		return nil
	}
	if d := e.dropped.Load(); d > 0 {
		e.ch <- Event{
			TS:	time.Now().UTC(),
			Node:	e.nodeName,
			Kind:	"events_dropped",
			Fields:	map[string]any{"count": d},
		}
	}
	close(e.ch)
	e.wg.Wait()
	return nil
}

var defaultLog atomic.Pointer[EventLog]

func SetDefault(e *EventLog)	{ defaultLog.Store(e) }

func Default() *EventLog	{ return defaultLog.Load() }

func Emit(kind string, fields map[string]any) {
	if l := defaultLog.Load(); l != nil {
		l.Emit(kind, fields)
	}
}
