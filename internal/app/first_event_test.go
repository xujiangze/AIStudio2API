package app

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

// TestFirstEventDeadline 首事件到达后解除期限，不取消后续长流
func TestFirstEventDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := newFirstEventContext(context.Background(), time.Minute)
		defer cancel()
		ctx.arm()
		time.Sleep(20 * time.Second)
		if !ctx.received() {
			t.Fatal("first event rejected before the deadline")
		}
		time.Sleep(2 * time.Minute)
		if ctx.Err() != nil {
			t.Fatalf("stream canceled after its first event: %v", ctx.Err())
		}
	})
}

// TestFirstEventDeadlineAcrossAttempts 换号和补发不延长首事件总期限
func TestFirstEventDeadlineAcrossAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := newFirstEventContext(context.Background(), time.Minute)
		defer cancel()
		ctx.arm()
		time.Sleep(40 * time.Second)
		ctx.arm()
		time.Sleep(20 * time.Second)
		synctest.Wait()
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatalf("deadline was reset for another attempt: %v", ctx.Err())
		}
		if ctx.received() {
			t.Fatal("accepted a first event after timeout")
		}
	})
}

// TestFirstEventDeadlineDisabled 首次排队、关闭期限和被排除的调用不触发计时
func TestFirstEventDeadlineDisabled(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			timeout := time.Minute
			if disabled {
				timeout = 0
			}
			ctx, cancel := newFirstEventContext(context.Background(), timeout)
			defer cancel()
			time.Sleep(2 * time.Minute)
			if ctx.Err() != nil {
				t.Fatalf("initial queue was timed: %v", ctx.Err())
			}
			ctx.arm()
			if !disabled {
				ctx.suspend()
			}
			time.Sleep(2 * time.Minute)
			if ctx.Err() != nil {
				t.Fatalf("disabled or excluded request timed out: %v", ctx.Err())
			}
		})
	}
}

// TestFirstEventDeadlineResume 原生调用失败后回退流式不会永久关闭首事件保护
func TestFirstEventDeadlineResume(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := newFirstEventContext(context.Background(), time.Minute)
		defer cancel()
		ctx.arm()
		time.Sleep(10 * time.Second)
		ctx.suspend()
		time.Sleep(20 * time.Second)
		ctx.arm()
		time.Sleep(30 * time.Second)
		synctest.Wait()
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatalf("stream fallback disabled the total deadline: %v", ctx.Err())
		}
	})
}

// TestFirstEventDeadlineParentCancel 首字之后仍遵守客户端取消
func TestFirstEventDeadlineParentCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		parent, stop := context.WithCancel(context.Background())
		ctx, cancel := newFirstEventContext(parent, time.Minute)
		defer cancel()
		ctx.arm()
		ctx.received()
		stop()
		synctest.Wait()
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatalf("parent cancellation lost: %v", ctx.Err())
		}
	})
}
