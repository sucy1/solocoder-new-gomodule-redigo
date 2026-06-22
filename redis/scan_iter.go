// Copyright 2012 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package redis

import (
	"context"
	"errors"
	"strconv"
	"sync"
)

// ErrNoMoreItems is returned by ScanIterator.Next when a full iteration
// round is complete (the server returned cursor 0). The caller can start
// a new iteration round by calling Next again, which will issue a fresh
// SCAN starting from cursor 0.
var ErrNoMoreItems = errors.New("redigo: scan iteration round complete")

// ScanIterator provides an iterator interface for Redis SCAN, SSCAN, HSCAN,
// and ZSCAN commands. It automatically handles cursor pagination until the
// server returns cursor 0, which signals the end of one complete iteration
// round over the collection.
//
// # Concurrency
//
// A ScanIterator is NOT safe for concurrent use. Callers must synchronize
// access externally if sharing across goroutines. Calling Next from multiple
// goroutines without external synchronization results in undefined behavior.
//
// # Iteration Semantics
//
// Redis SCAN is a cursor-based iteration with the following guarantees:
//   - A full iteration returns all elements that were present in the collection
//     from start to end, but may also return duplicates or miss elements that
//     were added or deleted during the iteration.
//   - When the server returns cursor 0, one complete round is finished. This
//     does NOT mean the collection is empty; it means the server has visited
//     every slot in its internal hash table at least once.
//   - If elements are added during iteration, they may or may not be returned.
//     To capture all new elements, start a new iteration round after receiving
//     ErrNoMoreItems.
//
// Example usage:
//
//	iter := redis.NewScanIterator(c, "MATCH", "user:*", "COUNT", 100)
//	for {
//	    keys, err := iter.Next()
//	    if err == redis.ErrNoMoreItems {
//	        break
//	    }
//	    if err != nil {
//	        // handle error
//	    }
//	    for _, key := range keys {
//	        // process key
//	    }
//	}
type ScanIterator struct {
	mu      sync.Mutex
	c       Conn
	ctx     context.Context
	cmd     string
	prefix  []interface{}
	args    []interface{}
	cursor  int64
	batch   []interface{}
	idx     int
	done    bool
	withCtx bool
}

// newScanIterator creates a new ScanIterator with the given connection, command, and arguments.
func newScanIterator(c Conn, ctx context.Context, cmd string, prefix []interface{}, args ...interface{}) *ScanIterator {
	return &ScanIterator{
		c:       c,
		ctx:     ctx,
		cmd:     cmd,
		prefix:  prefix,
		args:    args,
		cursor:  0,
		done:    false,
		withCtx: ctx != nil,
	}
}

// NewScanIterator returns a new ScanIterator for the Redis SCAN command.
// The SCAN command incrementally iterates over the collection of keys in the database.
func NewScanIterator(c Conn, args ...interface{}) *ScanIterator {
	return newScanIterator(c, nil, "SCAN", nil, args...)
}

// NewScanIteratorContext returns a new ScanIterator for the Redis SCAN command using context.
func NewScanIteratorContext(c Conn, ctx context.Context, args ...interface{}) *ScanIterator {
	return newScanIterator(c, ctx, "SCAN", nil, args...)
}

// NewSScanIterator returns a new ScanIterator for the Redis SSCAN command (set members).
func NewSScanIterator(c Conn, key string, args ...interface{}) *ScanIterator {
	return newScanIterator(c, nil, "SSCAN", []interface{}{key}, args...)
}

// NewSScanIteratorContext returns a new ScanIterator for the Redis SSCAN command using context.
func NewSScanIteratorContext(c Conn, ctx context.Context, key string, args ...interface{}) *ScanIterator {
	return newScanIterator(c, ctx, "SSCAN", []interface{}{key}, args...)
}

// NewHScanIterator returns a new ScanIterator for the Redis HSCAN command (hash fields/values).
func NewHScanIterator(c Conn, key string, args ...interface{}) *ScanIterator {
	return newScanIterator(c, nil, "HSCAN", []interface{}{key}, args...)
}

// NewHScanIteratorContext returns a new ScanIterator for the Redis HSCAN command using context.
func NewHScanIteratorContext(c Conn, ctx context.Context, key string, args ...interface{}) *ScanIterator {
	return newScanIterator(c, ctx, "HSCAN", []interface{}{key}, args...)
}

// NewZScanIterator returns a new ScanIterator for the Redis ZSCAN command (sorted set members/scores).
func NewZScanIterator(c Conn, key string, args ...interface{}) *ScanIterator {
	return newScanIterator(c, nil, "ZSCAN", []interface{}{key}, args...)
}

// NewZScanIteratorContext returns a new ScanIterator for the Redis ZSCAN command using context.
func NewZScanIteratorContext(c Conn, ctx context.Context, key string, args ...interface{}) *ScanIterator {
	return newScanIterator(c, ctx, "ZSCAN", []interface{}{key}, args...)
}

func (it *ScanIterator) execute() error {
	var reply interface{}
	var err error

	args := make([]interface{}, 0, len(it.prefix)+1+len(it.args))
	args = append(args, it.prefix...)
	args = append(args, it.cursor)
	args = append(args, it.args...)

	if it.withCtx {
		cwc, ok := it.c.(ConnWithContext)
		if !ok {
			return errContextNotSupported
		}
		reply, err = cwc.DoContext(it.ctx, it.cmd, args...)
	} else {
		reply, err = it.c.Do(it.cmd, args...)
	}
	if err != nil {
		return err
	}

	results, err := Values(reply, nil)
	if err != nil {
		return err
	}
	if len(results) != 2 {
		return errors.New("redigo: unexpected scan reply length")
	}

	cursorBytes, ok := results[0].([]byte)
	if !ok {
		return errors.New("redigo: unexpected scan cursor type")
	}
	it.cursor, err = strconv.ParseInt(string(cursorBytes), 10, 64)
	if err != nil {
		return err
	}

	it.batch, err = Values(results[1], nil)
	if err != nil {
		return err
	}
	it.idx = 0

	if it.cursor == 0 {
		it.done = true
	}

	return nil
}

// Next returns the next batch of results from the scan. It issues SCAN commands
// to the server as needed to fill each batch.
//
// When the server signals that one complete iteration round is finished by
// returning cursor 0, Next returns ErrNoMoreItems. The caller can call
// Next again after receiving ErrNoMoreItems to start a fresh iteration round,
// which is useful when elements may have been added during the previous round.
//
// This method is NOT safe for concurrent use.
func (it *ScanIterator) Next() ([]interface{}, error) {
	it.mu.Lock()
	defer it.mu.Unlock()

	for {
		if it.idx < len(it.batch) {
			results := it.batch[it.idx:]
			it.idx = len(it.batch)
			return results, nil
		}

		if it.done {
			it.done = false
			it.cursor = 0
			it.batch = nil
			it.idx = 0
			return nil, ErrNoMoreItems
		}

		if err := it.execute(); err != nil {
			return nil, err
		}
	}
}

// Done returns true if the current iteration round is complete (the server
// returned cursor 0) and all buffered items have been consumed. A new round
// can be started by calling Next again.
func (it *ScanIterator) Done() bool {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.done && it.idx >= len(it.batch)
}
