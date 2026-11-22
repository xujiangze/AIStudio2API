package app

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/aistudio"
	"github.com/Mag1cFall/AIStudio2API/internal/config"
)

// firstEventPolicy 是新请求取得后不再改变的首事件策略
type firstEventPolicy struct {
	auto         bool
	after        time.Duration
	models       []string
	timeout      time.Duration
	raceCooldown time.Duration
	busyCooldown time.Duration
}

type generationPolicyKey struct{}

// generationPolicy 的换号计数由竞速结算与续写共用，设置只在请求开始读取一次
type generationPolicy struct {
	first       firstEventPolicy
	maxSwitches int
	failures    atomic.Int64
}

// SetFirstEventPolicy 更新后续请求的策略和后续备用请求的全局放行上限
func (service *trackedService) SetFirstEventPolicy(cfg config.Config) {
	auto, after, _ := config.ParseFirstEventHedge(cfg.FirstEventHedge)
	service.firstPolicy.Store(&firstEventPolicy{
		auto: auto, after: after, models: config.ModelList(cfg.FirstEventHedgeModels),
		timeout:      time.Duration(cfg.FirstEventTimeoutSeconds) * time.Second,
		raceCooldown: time.Duration(cfg.FirstEventRaceCooldownSeconds) * time.Second,
		busyCooldown: time.Duration(cfg.ModelBusyCooldownSeconds) * time.Second,
	})
	service.hedgeLimit.Store(int64(cfg.FirstEventHedgeMaxConcurrency))
}

func (service *trackedService) generationContext(ctx context.Context) context.Context {
	policy := firstEventPolicy{}
	if saved := service.firstPolicy.Load(); saved != nil {
		policy = *saved
	}
	return context.WithValue(ctx, generationPolicyKey{}, &generationPolicy{first: policy, maxSwitches: int(service.maxAccountSwitches.Load())})
}

func generationSettings(ctx context.Context) *generationPolicy {
	policy, _ := ctx.Value(generationPolicyKey{}).(*generationPolicy)
	return policy
}

func (service *trackedService) canSwitchAccount(ctx context.Context) bool {
	if policy := generationSettings(ctx); policy != nil {
		return policy.failures.Load() <= int64(policy.maxSwitches)
	}
	return true
}

func (service *trackedService) recordAccountFailure(ctx context.Context, failures *int) bool {
	if policy := generationSettings(ctx); policy != nil {
		*failures = int(policy.failures.Add(1))
		return *failures <= policy.maxSwitches
	}
	*failures++
	return int64(*failures) <= service.maxAccountSwitches.Load()
}

// reserveAttemptFailure 在慢认证和文件清理前计入失败，防止胜出流提前使用未结算的预算
func (service *trackedService) reserveAttemptFailure(ctx context.Context, attempt *generateAttempt) {
	if attempt.budgetCounted || ctx.Err() != nil || !retryableGenerateAccountError(ctx, attempt.err) {
		return
	}
	if _, quota := aistudio.QuotaCooldownForError(attempt.err, time.Now()); quota || errors.Is(attempt.err, aistudio.ErrAccountCoolingDown) {
		return
	}
	workerFailed := service.workers.WorkerFailed(attempt.serving.Load().Account().ID)
	recoveryExpected := needsWAARuntimeRecovery(attempt.err, workerFailed, errors.Is(attempt.err, errAccountWorkerReplaced)) || errors.Is(attempt.err, aistudio.ErrProtectedTransport)
	if recoveryExpected {
		return
	}
	if attempt.serving.Load().Channel() == aistudio.ChannelBuild {
		var rpc *aistudio.RPCError
		if errors.As(attempt.err, &rpc) && rpc.StatusCode == 400 && rpc.Code == 3 {
			return
		}
	}
	service.recordAccountFailure(ctx, &attempt.failures)
	attempt.budgetCounted = true
}

func (policy firstEventPolicy) hedgesModel(modelID string) bool {
	return (policy.auto || policy.after > 0) && (slices.Contains(policy.models, "*") || slices.Contains(policy.models, modelID))
}

func (service *trackedService) firstEventStreaming(ctx context.Context, request aistudio.GenerateRequest, lease *aistudio.AccountLease) bool {
	if request.Background || service.pool.AssistantModel(request.Model) || service.pool.InteractionAgentModel(request.Model) {
		return false
	}
	if modes, ok := service.service.(interface {
		FirstEventStreaming(context.Context, aistudio.GenerateRequest, *aistudio.AccountLease) bool
	}); ok {
		return modes.FirstEventStreaming(ctx, request, lease)
	}
	return lease.Channel() != aistudio.ChannelBuild
}

func (service *trackedService) readyWarmAccountIDs() []string {
	return service.workers.ReadyWarmAccountIDs()
}

func (service *trackedService) markShortCooldown(requestID, modelID string, lease *aistudio.AccountLease, kind aistudio.ShortCooldownKind, duration time.Duration, reason string) error {
	if duration <= 0 {
		return nil
	}
	until, err := service.pool.MarkShortCooldown(lease.Account().ID, lease.Channel(), modelID, kind, time.Now(), duration, reason)
	if err == nil {
		service.requests.logRequestProgress(requestID, lease.Account().Config.Label, "WARN", "账号短冷却",
			"kind", kind, "channel", lease.Channel(), "model", modelID, "until", until.Local().Format(time.RFC3339), "reason", reason)
	}
	return err
}

func (service *trackedService) coolBusyAccount(ctx context.Context, requestID, modelID string, lease *aistudio.AccountLease, err error) error {
	if policy := generationSettings(ctx); policy != nil && aistudio.ModelBusyError(err) {
		return service.markShortCooldown(requestID, modelID, lease, aistudio.ShortCooldownBusy, policy.first.busyCooldown, "模型繁忙: "+err.Error())
	}
	return nil
}
