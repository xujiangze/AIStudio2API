package aistudio

import (
	"context"
	"errors"
	"net/http"
	"strings"
)

// ErrFirstEventNonstreaming 表示仅允许逐步首事件的调用在解析实际目录后被安全排除；不是账户或上游故障。
var ErrFirstEventNonstreaming = errors.New("generation does not provide streaming first events")

type streamingOnlyContextKey struct{}

// ContextWithStreamingOnly 要求 Generate 在解析实际目录后、准备或发送生成请求前排除非流式路径，不强制切换传输模式。
func ContextWithStreamingOnly(ctx context.Context) context.Context {
	return context.WithValue(ctx, streamingOnlyContextKey{}, true)
}

// FirstEventStreaming 只读租约账户已载入的目录，判断 Generate 能否逐步返回首事件。
// Build 原生单次调用、后台任务、助手路由及无法确定实际目录条目时均返回 false；不载入目录或改变发送模式。
func (s *PooledService) FirstEventStreaming(ctx context.Context, request GenerateRequest, lease *AccountLease) bool {
	if lease == nil || lease.Account() == nil {
		return false
	}
	return s.FirstEventStreamingForAccount(ctx, request, lease.Account().ID, lease.Channel())
}

// FirstEventStreamingForAccount 是获取候选租约前的同等模式预筛，accountID 与 channel 为待选目标。
// 只读取 Client 缓存，不获取账户池锁；取得租约后应再用 FirstEventStreaming 复核。
func (s *PooledService) FirstEventStreamingForAccount(ctx context.Context, request GenerateRequest, accountID string, channel Channel) bool {
	streaming, _ := s.FirstEventStreamingMode(ctx, request, accountID, channel)
	return streaming
}

// FirstEventStreamingMode 只读目标账户缓存，区分已知非流式与尚不能确定的模式。
// known 为 false 时不可猜测 Build fallback：实际目录重载后可能优先选中权益条目。
// 只读预筛允许未知备用候选时，应在 Generate 的上下文附加 ContextWithStreamingOnly 以确保不发原生单次调用。
func (s *PooledService) FirstEventStreamingMode(_ context.Context, request GenerateRequest, accountID string, channel Channel) (streaming bool, known bool) {
	if s == nil || s.client == nil || accountID == "" {
		return false, false
	}
	// 与 modelEntry 使用相同的规范化及 lookup；不能用其他账户目录或 Build 目录覆盖优先的 Playground 条目。
	s.client.catalogMu.RLock()
	defer s.client.catalogMu.RUnlock()
	entry, exists := s.client.catalogs[accountID].lookup(strings.TrimPrefix(request.Model, "models/"))
	if !exists {
		// modelEntry 在缺目录或缺模型时会重新载入页面；Build-only fallback 前也会经过这一步，当前无法确定。
		return false, false
	}
	return generateFirstEventStreaming(request, entry, channel), true
}

// generateFirstEventStreaming 根据实际目录条目与通道纯判断首事件是否可逐步返回，不代表请求或候选账户可用。
func generateFirstEventStreaming(request GenerateRequest, entry modelEntry, channel Channel) bool {
	if request.Background || entry.model.Capabilities["assistant_route"] || entry.defaults.InteractionTask {
		return false
	}
	switch channel {
	case ChannelPlayground:
		return true
	case ChannelBuild:
		return !buildRequestUsesUnary(request, entry.model)
	default:
		return false
	}
}

// ModelBusyError 仅识别 HTTP 503、协议错误码 14 且明确说明模型高需求或过载的错误，不扩展额度或通用 5xx 分类。
func ModelBusyError(err error) bool {
	var rpcErr *RPCError
	if !errors.As(err, &rpcErr) || rpcErr == nil || rpcErr.StatusCode != http.StatusServiceUnavailable || rpcErr.Code != 14 {
		return false
	}
	message := strings.ToLower(rpcErr.Message)
	for _, evidence := range []string{
		"model is overloaded",
		"model is currently overloaded",
		"model overloaded",
		"model is experiencing high demand",
		"model is currently experiencing high demand",
	} {
		if strings.Contains(message, evidence) {
			return true
		}
	}
	return false
}
