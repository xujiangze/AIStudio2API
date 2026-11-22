package app

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// firstEventTimeoutError 区分首事件总期限与客户端取消，兼容现有 504 映射
type firstEventTimeoutError struct{ timeout time.Duration }

func (err *firstEventTimeoutError) Error() string {
	return fmt.Sprintf("Upstream first event timed out after %s", err.timeout)
}

func (*firstEventTimeoutError) Unwrap() error { return context.DeadlineExceeded }

type firstEventGuardKey struct{}

func firstEventGuard(ctx context.Context) *firstEventContext {
	guard, _ := ctx.Value(firstEventGuardKey{}).(*firstEventContext)
	return guard
}

// firstEventContext 只在等首事件时计时，不把可撤销期限传给网络层作为固定读期限
type firstEventContext struct {
	context.Context
	cancel    context.CancelCauseFunc
	timeout   time.Duration
	mu        sync.Mutex
	timer     *time.Timer
	version   uint64
	started   bool
	startedAt time.Time
	finished  bool
	paused    bool
}

func newFirstEventContext(parent context.Context, timeout time.Duration) (*firstEventContext, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(parent)
	guard := &firstEventContext{Context: ctx, cancel: cancel, timeout: timeout}
	return guard, func() {
		guard.suspend()
		cancel(context.Canceled)
	}
}

func (ctx *firstEventContext) begin() {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if !ctx.started {
		ctx.started = true
		ctx.startedAt = time.Now()
		ctx.paused = true
	}
}

// arm 首次取得账户后开始计时，换号与备用补发不会重置
func (ctx *firstEventContext) arm() {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if ctx.finished || ctx.timeout <= 0 || ctx.Context.Err() != nil {
		return
	}
	if ctx.started && !ctx.paused {
		return
	}
	if !ctx.started {
		ctx.started = true
		ctx.startedAt = time.Now()
	}
	ctx.paused = false
	remaining := ctx.timeout - time.Since(ctx.startedAt)
	if remaining <= 0 {
		ctx.cancel(&firstEventTimeoutError{timeout: ctx.timeout})
		return
	}
	ctx.version++
	version := ctx.version
	ctx.timer = time.AfterFunc(remaining, func() {
		ctx.mu.Lock()
		defer ctx.mu.Unlock()
		if !ctx.finished && !ctx.paused && ctx.version == version {
			ctx.cancel(&firstEventTimeoutError{timeout: ctx.timeout})
		}
	})
}

// received 与超时仲裁，只撤销计时而不取消胜出请求的上下文
func (ctx *firstEventContext) received() bool {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	if ctx.Context.Err() != nil {
		return false
	}
	ctx.finished = true
	if ctx.timer != nil {
		ctx.timer.Stop()
	}
	return true
}

// suspend 排除当前原生非流式调用，回退流式时仍按首次尝试的时间计算
func (ctx *firstEventContext) suspend() {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	ctx.paused = true
	ctx.version++
	if ctx.timer != nil {
		ctx.timer.Stop()
	}
}

func (ctx *firstEventContext) Value(key any) any {
	if _, ok := key.(firstEventGuardKey); ok {
		return ctx
	}
	return ctx.Context.Value(key)
}

func (ctx *firstEventContext) Err() error {
	if cause := context.Cause(ctx.Context); cause != nil {
		if _, ok := cause.(*firstEventTimeoutError); ok {
			return cause
		}
	}
	return ctx.Context.Err()
}
