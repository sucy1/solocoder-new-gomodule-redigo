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
	"fmt"
	"sync"
	"time"
)

// BlockingCmd represents a blocking Redis command that should be automatically
// re-issued after a reconnection. It stores the command name and arguments
// so they can be replayed on a new connection.
type BlockingCmd struct {
	// Cmd is the Redis command name (e.g., "BLPOP", "BRPOP", "BRPOPLPUSH").
	Cmd string

	// Args are the command arguments excluding the command name itself.
	// For BLPOP "mylist" 0, Args would be []interface{}{"mylist", 0}.
	Args []interface{}
}

// AutoReconnectPubSub wraps a PubSubConn with automatic reconnection support.
// When the connection is lost, it will automatically reconnect and re-subscribe
// to all previously subscribed channels and patterns.
//
// # Reconnection and blocking commands
//
// AutoReconnectPubSub automatically restores SUBSCRIBE and PSUBSCRIBE state
// after a reconnection. For blocking commands such as BLPOP, BRPOP, and
// BRPOPLPUSH, you have two options:
//
//  1. Use AddBlockingCmd() to register commands that should be automatically
//     re-issued after each successful reconnection. This is the recommended
//     approach for most use cases.
//
//  2. Use the OnReconnect callback for full control over the reconnection
//     process. Use this for more complex recovery logic.
//
// Example:
//
//	psc := redis.NewAutoReconnectPubSub(dialFn)
//	defer psc.Close()
//
//	// Register blocking commands to auto-restore after reconnection
//	psc.AddBlockingCmd("BLPOP", "work_queue", 0)
//	psc.AddBlockingCmd("BRPOP", "high_priority_queue", 60)
//
//	// Subscribe to channels (auto-restored)
//	psc.Subscribe("notifications")
type AutoReconnectPubSub struct {
	dialFn      func() (Conn, error)
	dialCtxFn   func(ctx context.Context) (Conn, error)
	mu          sync.Mutex
	psc         PubSubConn
	channels    map[string]struct{}
	patterns    map[string]struct{}
	blockingCmds []BlockingCmd
	reconnect   bool
	maxAttempts int
	backoff     time.Duration

	// OnReconnect is an optional callback invoked after a successful reconnection.
	// It is called with the newly established connection after all previously
	// subscribed channels, patterns, and registered blocking commands have
	// been restored.
	//
	// Use this callback for application-specific recovery logic beyond what
	// AddBlockingCmd provides.
	//
	// The callback is invoked while holding the internal mutex, so it must NOT
	// call methods on the AutoReconnectPubSub itself (which would deadlock).
	// It should only use the provided Conn argument directly.
	OnReconnect func(conn Conn)
}

// NewAutoReconnectPubSub creates a new AutoReconnectPubSub using the given dial function.
func NewAutoReconnectPubSub(dialFn func() (Conn, error)) *AutoReconnectPubSub {
	return &AutoReconnectPubSub{
		dialFn:      dialFn,
		channels:    make(map[string]struct{}),
		patterns:    make(map[string]struct{}),
		blockingCmds: make([]BlockingCmd, 0),
		reconnect:   true,
		maxAttempts: 5,
		backoff:     100 * time.Millisecond,
	}
}

// NewAutoReconnectPubSubContext creates a new AutoReconnectPubSub using the given context-aware dial function.
func NewAutoReconnectPubSubContext(dialCtxFn func(ctx context.Context) (Conn, error)) *AutoReconnectPubSub {
	return &AutoReconnectPubSub{
		dialCtxFn:   dialCtxFn,
		channels:    make(map[string]struct{}),
		patterns:    make(map[string]struct{}),
		blockingCmds: make([]BlockingCmd, 0),
		reconnect:   true,
		maxAttempts: 5,
		backoff:     100 * time.Millisecond,
	}
}

// SetReconnect enables or disables automatic reconnection.
func (arpc *AutoReconnectPubSub) SetReconnect(enabled bool) {
	arpc.mu.Lock()
	arpc.reconnect = enabled
	arpc.mu.Unlock()
}

// SetMaxAttempts sets the maximum number of reconnection attempts per Receive call.
func (arpc *AutoReconnectPubSub) SetMaxAttempts(n int) {
	arpc.mu.Lock()
	arpc.maxAttempts = n
	arpc.mu.Unlock()
}

// SetBackoff sets the initial backoff duration between reconnection attempts.
// The backoff doubles with each attempt.
func (arpc *AutoReconnectPubSub) SetBackoff(d time.Duration) {
	arpc.mu.Lock()
	arpc.backoff = d
	arpc.mu.Unlock()
}

// AddBlockingCmd registers a blocking command to be automatically re-issued
// after each successful reconnection. The command is issued on the new
// connection after SUBSCRIBE/PSUBSCRIBE state has been restored, but before
// the OnReconnect callback (if any) is invoked.
//
// Commands are issued in the order they were added. If any command fails,
// the reconnection attempt is considered failed and the connection is closed.
//
// Example:
//
//	psc.AddBlockingCmd("BLPOP", "work_queue", 0)
//	psc.AddBlockingCmd("BRPOP", "high_priority", 30)
func (arpc *AutoReconnectPubSub) AddBlockingCmd(cmd string, args ...interface{}) {
	arpc.mu.Lock()
	defer arpc.mu.Unlock()
	arpc.blockingCmds = append(arpc.blockingCmds, BlockingCmd{Cmd: cmd, Args: args})
}

// RemoveBlockingCmd removes all registered blocking commands matching the given
// command name. If no command name is provided, all blocking commands are removed.
func (arpc *AutoReconnectPubSub) RemoveBlockingCmd(cmdName ...string) {
	arpc.mu.Lock()
	defer arpc.mu.Unlock()

	if len(cmdName) == 0 {
		arpc.blockingCmds = make([]BlockingCmd, 0)
		return
	}

	keep := make([]BlockingCmd, 0, len(arpc.blockingCmds))
	removeSet := make(map[string]struct{}, len(cmdName))
	for _, name := range cmdName {
		removeSet[name] = struct{}{}
	}
	for _, cmd := range arpc.blockingCmds {
		if _, remove := removeSet[cmd.Cmd]; !remove {
			keep = append(keep, cmd)
		}
	}
	arpc.blockingCmds = keep
}

// BlockingCmds returns a copy of the currently registered blocking commands.
func (arpc *AutoReconnectPubSub) BlockingCmds() []BlockingCmd {
	arpc.mu.Lock()
	defer arpc.mu.Unlock()
	result := make([]BlockingCmd, len(arpc.blockingCmds))
	copy(result, arpc.blockingCmds)
	return result
}

func (arpc *AutoReconnectPubSub) connect() (Conn, error) {
	if arpc.dialCtxFn != nil {
		return arpc.dialCtxFn(context.Background())
	}
	return arpc.dialFn()
}

func (arpc *AutoReconnectPubSub) ensureConn() error {
	if arpc.psc.Conn != nil && arpc.psc.Conn.Err() == nil {
		return nil
	}
	conn, err := arpc.connect()
	if err != nil {
		return err
	}
	arpc.psc = PubSubConn{Conn: conn}

	if len(arpc.channels) > 0 {
		chans := make([]interface{}, 0, len(arpc.channels))
		for ch := range arpc.channels {
			chans = append(chans, ch)
		}
		if err := arpc.psc.Subscribe(chans...); err != nil {
			conn.Close()
			arpc.psc = PubSubConn{}
			return err
		}
	}

	if len(arpc.patterns) > 0 {
		pats := make([]interface{}, 0, len(arpc.patterns))
		for pat := range arpc.patterns {
			pats = append(pats, pat)
		}
		if err := arpc.psc.PSubscribe(pats...); err != nil {
			conn.Close()
			arpc.psc = PubSubConn{}
			return err
		}
	}

	for _, bc := range arpc.blockingCmds {
		args := make([]interface{}, len(bc.Args))
		copy(args, bc.Args)
		if _, err := conn.Do(bc.Cmd, args...); err != nil {
			conn.Close()
			arpc.psc = PubSubConn{}
			return fmt.Errorf("redigo: auto-reconnect failed to re-issue blocking command %s: %w", bc.Cmd, err)
		}
	}

	if arpc.OnReconnect != nil {
		arpc.OnReconnect(conn)
	}

	return nil
}

func (arpc *AutoReconnectPubSub) tryReconnect() error {
	arpc.mu.Lock()
	defer arpc.mu.Unlock()

	if !arpc.reconnect {
		if arpc.psc.Conn != nil {
			return arpc.psc.Conn.Err()
		}
		return nil
	}

	if arpc.psc.Conn != nil {
		_ = arpc.psc.Conn.Close()
		arpc.psc = PubSubConn{}
	}

	backoff := arpc.backoff
	var lastErr error
	attempts := arpc.maxAttempts
	if attempts <= 0 {
		attempts = 1
	}

	for i := 0; i < attempts; i++ {
		if err := arpc.ensureConn(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(backoff)
		backoff *= 2
	}

	return lastErr
}

// Close closes the connection.
func (arpc *AutoReconnectPubSub) Close() error {
	arpc.mu.Lock()
	defer arpc.mu.Unlock()
	arpc.reconnect = false
	if arpc.psc.Conn != nil {
		return arpc.psc.Close()
	}
	return nil
}

// Subscribe subscribes the connection to the specified channels.
func (arpc *AutoReconnectPubSub) Subscribe(channel ...interface{}) error {
	arpc.mu.Lock()
	defer arpc.mu.Unlock()

	if err := arpc.ensureConn(); err != nil {
		return err
	}

	for _, ch := range channel {
		if s, ok := ch.(string); ok {
			arpc.channels[s] = struct{}{}
		} else if b, ok := ch.([]byte); ok {
			arpc.channels[string(b)] = struct{}{}
		}
	}

	return arpc.psc.Subscribe(channel...)
}

// PSubscribe subscribes the connection to the given patterns.
func (arpc *AutoReconnectPubSub) PSubscribe(channel ...interface{}) error {
	arpc.mu.Lock()
	defer arpc.mu.Unlock()

	if err := arpc.ensureConn(); err != nil {
		return err
	}

	for _, pat := range channel {
		if s, ok := pat.(string); ok {
			arpc.patterns[s] = struct{}{}
		} else if b, ok := pat.([]byte); ok {
			arpc.patterns[string(b)] = struct{}{}
		}
	}

	return arpc.psc.PSubscribe(channel...)
}

// Unsubscribe unsubscribes the connection from the given channels, or from all
// of them if none is given.
func (arpc *AutoReconnectPubSub) Unsubscribe(channel ...interface{}) error {
	arpc.mu.Lock()
	defer arpc.mu.Unlock()

	if err := arpc.ensureConn(); err != nil {
		return err
	}

	if len(channel) == 0 {
		arpc.channels = make(map[string]struct{})
	} else {
		for _, ch := range channel {
			if s, ok := ch.(string); ok {
				delete(arpc.channels, s)
			} else if b, ok := ch.([]byte); ok {
				delete(arpc.channels, string(b))
			}
		}
	}

	return arpc.psc.Unsubscribe(channel...)
}

// PUnsubscribe unsubscribes the connection from the given patterns, or from all
// of them if none is given.
func (arpc *AutoReconnectPubSub) PUnsubscribe(channel ...interface{}) error {
	arpc.mu.Lock()
	defer arpc.mu.Unlock()

	if err := arpc.ensureConn(); err != nil {
		return err
	}

	if len(channel) == 0 {
		arpc.patterns = make(map[string]struct{})
	} else {
		for _, pat := range channel {
			if s, ok := pat.(string); ok {
				delete(arpc.patterns, s)
			} else if b, ok := pat.([]byte); ok {
				delete(arpc.patterns, string(b))
			}
		}
	}

	return arpc.psc.PUnsubscribe(channel...)
}

// Ping sends a PING to the server with the specified data.
func (arpc *AutoReconnectPubSub) Ping(data string) error {
	arpc.mu.Lock()
	defer arpc.mu.Unlock()

	if err := arpc.ensureConn(); err != nil {
		return err
	}

	return arpc.psc.Ping(data)
}

// Receive returns a pushed message as a Subscription, Message, Pong or error.
// If an error occurs due to connection issues, it will attempt to automatically
// reconnect and re-subscribe before returning the error.
func (arpc *AutoReconnectPubSub) Receive() interface{} {
	arpc.mu.Lock()
	if err := arpc.ensureConn(); err != nil {
		arpc.mu.Unlock()
		_ = arpc.tryReconnect()
		return err
	}
	arpc.mu.Unlock()

	reply := arpc.psc.Receive()
	if _, ok := reply.(error); ok {
		_ = arpc.tryReconnect()
	}
	return reply
}

// ReceiveWithTimeout is like Receive, but it allows the application to
// override the connection's default timeout.
func (arpc *AutoReconnectPubSub) ReceiveWithTimeout(timeout time.Duration) interface{} {
	arpc.mu.Lock()
	if err := arpc.ensureConn(); err != nil {
		arpc.mu.Unlock()
		_ = arpc.tryReconnect()
		return err
	}
	arpc.mu.Unlock()

	reply := arpc.psc.ReceiveWithTimeout(timeout)
	if _, ok := reply.(error); ok {
		_ = arpc.tryReconnect()
	}
	return reply
}

// ReceiveContext is like Receive, but it allows termination of the receive
// via a Context. If the call returns due to closure of the context's Done
// channel the underlying Conn will have been closed.
func (arpc *AutoReconnectPubSub) ReceiveContext(ctx context.Context) interface{} {
	arpc.mu.Lock()
	if err := arpc.ensureConn(); err != nil {
		arpc.mu.Unlock()
		_ = arpc.tryReconnect()
		return err
	}
	arpc.mu.Unlock()

	reply := arpc.psc.ReceiveContext(ctx)
	if _, ok := reply.(error); ok {
		_ = arpc.tryReconnect()
	}
	return reply
}
