package app

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Mag1cFall/AIStudio2API/internal/aistudio"
	"github.com/Mag1cFall/AIStudio2API/internal/api"
)

// firstEventSampleLimit 是每个模型与通道保留的近期原请求首事件样本数
const firstEventSampleLimit = 200

// firstEventSampleAge 是首事件样本参与统计的最长时间
const firstEventSampleAge = 24 * time.Hour

// firstEventMinSamples 是自适应阈值需要的最少样本数，不足时不发起备用请求
const firstEventMinSamples = 20

// firstEventKey 是首事件样本所属的模型与通道
type firstEventKey struct {
	model   string
	channel string
}

// firstEventSample 是一次原请求从开始发送到首事件或被备用请求取代的时长
type firstEventSample struct {
	at   time.Time
	wait time.Duration
}

// firstEventStats 按模型与通道保存近期原请求的首事件等待，备用请求不计入
type firstEventStats struct {
	mu      sync.Mutex
	samples map[firstEventKey][]firstEventSample
}

func newFirstEventStats() *firstEventStats {
	return &firstEventStats{samples: make(map[firstEventKey][]firstEventSample)}
}

// observe 记录一个原请求样本，每个模型与通道只保留最近 firstEventSampleLimit 个
func (stats *firstEventStats) observe(model, channel string, at time.Time, wait time.Duration) {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	key := firstEventKey{model: model, channel: channel}
	samples := append(stats.samples[key], firstEventSample{at: at, wait: wait})
	stats.samples[key] = samples[max(0, len(samples)-firstEventSampleLimit):]
}

// p95 返回模型与通道 firstEventSampleAge 内样本的 p95 与样本数
func (stats *firstEventStats) p95(model, channel string, now time.Time) (time.Duration, int) {
	stats.mu.Lock()
	waits := recentWaits(stats.samples[firstEventKey{model: model, channel: channel}], now)
	stats.mu.Unlock()
	return percentile95(waits), len(waits)
}

// latencies 返回各模型与通道近期样本的 p95，按模型与通道排序
func (stats *firstEventStats) latencies(now time.Time) []api.FirstEventLatency {
	stats.mu.Lock()
	defer stats.mu.Unlock()
	latencies := make([]api.FirstEventLatency, 0, len(stats.samples))
	for key, samples := range stats.samples {
		waits := recentWaits(samples, now)
		if len(waits) == 0 {
			continue
		}
		latencies = append(latencies, api.FirstEventLatency{
			Model: key.model, Channel: key.channel, Samples: len(waits), P95MS: percentile95(waits).Milliseconds(),
		})
	}
	slices.SortFunc(latencies, func(a, b api.FirstEventLatency) int {
		return cmp.Or(strings.Compare(a.Model, b.Model), strings.Compare(a.Channel, b.Channel))
	})
	return latencies
}

// seed 从请求账本的近期记录补入原请求样本：被备用请求取代的原请求取其已等时长，其余只含落败备用请求或没有尝试记录的取首事件减排队
func (stats *firstEventStats) seed(records []api.UsageRecord) {
	slices.SortFunc(records, func(a, b api.UsageRecord) int { return a.Time.Compare(b.Time) })
	for _, record := range records {
		model := strings.TrimPrefix(record.Model, "models/")
		superseded := slices.IndexFunc(record.Attempts, func(attempt api.RequestAttempt) bool { return attempt.Superseded && !attempt.Hedge })
		switch {
		case superseded >= 0:
			attempt := record.Attempts[superseded]
			stats.observe(model, attempt.Channel, record.Time, time.Duration(attempt.DurationMS)*time.Millisecond)
		case record.FirstEventMS > 0 && !slices.ContainsFunc(record.Attempts, func(attempt api.RequestAttempt) bool { return !attempt.Hedge }):
			stats.observe(model, record.Channel, record.Time, time.Duration(record.FirstEventMS-record.QueueMS)*time.Millisecond)
		}
	}
}

// recentWaits 返回 firstEventSampleAge 内的样本时长
func recentWaits(samples []firstEventSample, now time.Time) []time.Duration {
	waits := make([]time.Duration, 0, len(samples))
	for _, sample := range samples {
		if now.Sub(sample.at) <= firstEventSampleAge {
			waits = append(waits, sample.wait)
		}
	}
	return waits
}

// percentile95 按最近秩法返回时长的 p95，没有样本时为 0
func percentile95(waits []time.Duration) time.Duration {
	if len(waits) == 0 {
		return 0
	}
	slices.Sort(waits)
	return waits[(len(waits)*95+99)/100-1]
}

// hedgeDelay 返回快照策略对本模型与通道的竞速阈值
func (service *trackedService) hedgeDelay(policy firstEventPolicy, modelID, channel string) (time.Duration, bool) {
	if !policy.auto {
		return policy.after, policy.after > 0
	}
	p95, samples := service.requests.firstEvents.p95(modelID, channel, time.Now())
	return p95, samples >= firstEventMinSamples
}

// hedgeSelections 只选择尚未尝试的其他账户，固定账户或文件绑定不改换归属
func (service *trackedService) hedgeSelections(primary aistudio.AccountSelection, accountID string, fixed bool, attempted map[string]struct{}) []aistudio.AccountSelection {
	if fixed {
		return nil
	}
	other := primary
	other.AccountID = ""
	other.PreferredAccountID = ""
	other.AllowedAccountIDs = []string{}
	for _, id := range service.pool.EnabledAccounts() {
		if _, tried := attempted[id]; !tried && id != accountID && (primary.AllowedAccountIDs == nil || slices.Contains(primary.AllowedAccountIDs, id)) {
			other.AllowedAccountIDs = append(other.AllowedAccountIDs, id)
		}
	}
	return []aistudio.AccountSelection{other}
}

// tryHedgeLease 使用已经就绪且仍有槽位的不同账户，不为备用请求冷启动 Worker
func (service *trackedService) tryHedgeLease(ctx context.Context, request aistudio.GenerateRequest, selections []aistudio.AccountSelection) (*aistudio.AccountLease, aistudio.AccountSelection) {
	for _, selection := range selections {
		if len(selection.AllowedAccountIDs) == 0 {
			continue
		}
		groups, err := service.pool.ClassifyCandidates(ctx, selection, service.readyWarmAccountIDs())
		if err != nil {
			continue
		}
		available := append(slices.Clone(groups.WarmReady), groups.WarmAvailable...)
		for _, id := range available {
			candidate := selection
			candidate.AccountID = id
			candidate.AllowedAccountIDs = []string{id}
			if modes, ok := service.service.(interface {
				FirstEventStreamingMode(context.Context, aistudio.GenerateRequest, string, aistudio.Channel) (bool, bool)
			}); ok && candidate.Channel != "" {
				if streaming, known := modes.FirstEventStreamingMode(ctx, request, id, candidate.Channel); known && !streaming {
					continue
				}
			}
			lease, _, err := service.pool.TryAcquireFor(ctx, candidate)
			if err != nil || lease == nil {
				continue
			}
			if modes, ok := service.service.(interface {
				FirstEventStreamingMode(context.Context, aistudio.GenerateRequest, string, aistudio.Channel) (bool, bool)
			}); ok {
				streaming, known := modes.FirstEventStreamingMode(ctx, request, id, lease.Channel())
				if known && !streaming {
					_ = lease.Release()
					continue
				}
			} else if !service.firstEventStreaming(ctx, request, lease) {
				_ = lease.Release()
				continue
			}
			return lease, candidate
		}
	}
	return nil, aistudio.AccountSelection{}
}

type hedgePlan struct {
	delay      time.Duration
	selections func() []aistudio.AccountSelection
	allowed    func() bool
	start      func(*aistudio.AccountLease, aistudio.AccountSelection) *generateAttempt
}

func (service *trackedService) reserveHedge() (func(), bool) {
	for {
		used := service.hedges.Load()
		if used >= service.hedgeLimit.Load() {
			return nil, false
		}
		if service.hedges.CompareAndSwap(used, used+1) {
			var once sync.Once
			return func() { once.Do(func() { service.hedges.Add(-1) }) }, true
		}
	}
}

// awaitFirstEvent 仲裁两份尝试；失败结算独立进行，不挡住存活尝试出首事件
func (service *trackedService) awaitFirstEvent(
	requestCtx context.Context,
	request aistudio.GenerateRequest,
	modelID string,
	primary *generateAttempt,
	plan *hedgePlan,
	retry *generateRetry,
	failed func(*generateAttempt, attemptOutcome),
) *generateAttempt {
	type settlement struct {
		attempt *generateAttempt
		outcome attemptOutcome
		retry   *generateRetry
	}
	primaryDone := primary.done
	var hedge *generateAttempt
	var hedgeDone <-chan struct{}
	results := make(chan settlement, 2)
	pending := 0
	settling := false
	allowMore := true
	var check <-chan time.Time
	var timer *time.Timer
	if plan != nil {
		timer = time.NewTimer(max(plan.delay-time.Since(primary.startedAt), 0))
		defer timer.Stop()
		if primary.streaming.Load() {
			check = timer.C
		}
	}
	startSettlement := func(attempt *generateAttempt, mayRecover bool) {
		pending++
		settling = true
		service.reserveAttemptFailure(requestCtx, attempt)
		snapshot := *retry
		snapshot.attempted = make(map[string]struct{}, len(retry.attempted))
		for id := range retry.attempted {
			snapshot.attempted[id] = struct{}{}
		}
		snapshot.recoveredWorker = make(map[string]struct{}, len(retry.recoveredWorker))
		for id := range retry.recoveredWorker {
			snapshot.recoveredWorker[id] = struct{}{}
		}
		go func() {
			outcome := service.settleAttempt(requestCtx, request.ID, modelID, attempt, &snapshot, mayRecover)
			results <- settlement{attempt: attempt, outcome: outcome, retry: &snapshot}
		}()
	}
	var queued []*generateAttempt
	var recoveries []settlement
	submitFailure := func(attempt *generateAttempt) {
		service.reserveAttemptFailure(requestCtx, attempt)
		if settling {
			queued = append(queued, attempt)
		} else {
			startSettlement(attempt, true)
		}
	}
	settleFinishedLoser := func(loser *generateAttempt) bool {
		select {
		case <-loser.done:
			if loser.err != nil {
				startSettlement(loser, false)
			} else {
				service.supersede(requestCtx, request.ID, loser, "另一尝试先返回首事件")
			}
			return true
		default:
			return false
		}
	}
	cancelPending := func() {
		if primaryDone != nil && !settleFinishedLoser(primary) {
			service.discardAttempt(requestCtx, request.ID, primary)
		}
		if hedgeDone != nil && !settleFinishedLoser(hedge) {
			service.discardAttempt(requestCtx, request.ID, hedge)
		}
		for _, attempt := range queued {
			startSettlement(attempt, false)
		}
	}
	for primaryDone != nil || hedgeDone != nil || pending > 0 || len(queued) > 0 {
		select {
		case <-primaryDone:
			primaryDone = nil
			check = nil
			if primary.err == nil {
				if guard, ok := requestCtx.(*firstEventContext); ok && !guard.received() {
					service.discardAttempt(requestCtx, request.ID, primary)
					cancelPending()
					return nil
				}
				service.requests.firstEvents.observe(modelID, string(primary.lease.Channel()), primary.firstAt, primary.firstAt.Sub(primary.startedAt))
				if hedgeDone != nil && !settleFinishedLoser(hedge) {
					service.supersede(requestCtx, request.ID, hedge, "原请求先返回首事件")
				}
				for _, attempt := range queued {
					startSettlement(attempt, false)
				}
				return primary
			}
			submitFailure(primary)
		case <-hedgeDone:
			hedgeDone = nil
			if hedge.err == nil {
				if guard, ok := requestCtx.(*firstEventContext); ok && !guard.received() {
					service.discardAttempt(requestCtx, request.ID, hedge)
					cancelPending()
					return nil
				}
				if hedge.hedgeRelease != nil {
					release := hedge.hedgeRelease
					hedge.hedgeRelease = nil
					go func() { <-primary.cleanupDone; release() }()
				}
				waited := time.Since(primary.startedAt)
				if primaryDone != nil && !settleFinishedLoser(primary) {
					service.requests.firstEvents.observe(modelID, string(primary.lease.Channel()), time.Now(), waited)
					if settings := generationSettings(requestCtx); settings != nil {
						if err := service.markShortCooldown(request.ID, modelID, primary.serving.Load(), aistudio.ShortCooldownRace, settings.first.raceCooldown, "竞速冷却: 备用请求先返回首事件"); err != nil {
							service.requests.logRequestProgress(request.ID, primary.accountLabel, "WARN", "账号短冷却失败", "error", err)
						}
					}
					service.supersede(requestCtx, request.ID, primary, "备用请求先返回首事件")
				}
				for _, attempt := range queued {
					startSettlement(attempt, false)
				}
				service.requests.logRequestProgress(request.ID, hedge.accountLabel, "INFO", "备用请求胜出", "primary_waited", waited, "model", modelID)
				return hedge
			}
			submitFailure(hedge)
		case result := <-results:
			pending--
			settling = false
			*retry = *result.retry
			if result.outcome.recoverWorker {
				recoveries = append(recoveries, result)
			}
			failed(result.attempt, result.outcome)
			allowMore = allowMore && result.outcome.retryable
			if len(queued) > 0 {
				attempt := queued[0]
				queued = queued[1:]
				startSettlement(attempt, true)
			}
			if plan != nil && primaryDone != nil && hedgeDone == nil && !settling && allowMore && plan.allowed() {
				check = time.After(0)
			}
		case <-primary.modeChanged:
			if plan != nil && primaryDone != nil && hedgeDone == nil {
				if primary.streaming.Load() {
					check = time.After(max(plan.delay-time.Since(primary.startedAt), 0))
				} else {
					check = nil
				}
			}
		case <-check:
			check = nil
			if primaryDone == nil || !primary.streaming.Load() || hedgeDone != nil || settling || !allowMore || !plan.allowed() {
				continue
			}
			release, ok := service.reserveHedge()
			if !ok {
				check = time.After(schedulingRecheck)
				continue
			}
			lease, selection := service.tryHedgeLease(requestCtx, request, plan.selections())
			if lease == nil {
				release()
				check = time.After(schedulingRecheck)
				continue
			}
			hedge = plan.start(lease, selection)
			hedge.hedgeRelease = release
			hedgeDone = hedge.done
			service.requests.logRequestProgress(request.ID, hedge.accountLabel, "WARN", "备用请求发起",
				"primary_waited", time.Since(primary.startedAt), "threshold", plan.delay, "model", modelID)
		case <-requestCtx.Done():
			cancelPending()
			return nil
		}
	}
	if len(recoveries) > 0 && requestCtx.Err() == nil && service.canSwitchAccount(requestCtx) {
		result := recoveries[0]
		failed(result.attempt, result.outcome)
	}
	return nil
}

// cleanupAttempt 只在上游尝试完全停止后释放文件、租约和备用槽位
func (service *trackedService) cleanupAttempt(attempt *generateAttempt) error {
	attempt.cleanupOnce.Do(func() {
		defer close(attempt.cleanupDone)
		attempt.cancel()
		<-attempt.done
		if attempt.source != nil {
			for range attempt.source {
			}
		}
		if attempt.copies != nil {
			attempt.cleanupErr = attempt.copies.Cleanup()
		}
		attempt.cleanupErr = errors.Join(attempt.cleanupErr, attempt.serving.Load().Release())
		if attempt.hedgeRelease != nil {
			attempt.hedgeRelease()
		}
	})
	return attempt.cleanupErr
}

func (service *trackedService) discardAttempt(requestCtx context.Context, requestID string, attempt *generateAttempt) {
	attempt.cancel()
	go func() {
		if err := service.cleanupAttempt(attempt); err != nil {
			service.requests.logRequestProgress(requestID, attempt.accountLabel, "WARN", "尝试清理失败", "error", err)
		}
	}()
}

// supersede 记为被取代而非失败，取消后在后台等上游与文件清理结束
func (service *trackedService) supersede(requestCtx context.Context, requestID string, loser *generateAttempt, reason string) {
	api.AddAccessLogAttempt(requestCtx, api.RequestAttempt{
		Account: loser.accountLabel, Channel: string(loser.lease.Channel()), Error: reason,
		DurationMS: time.Since(loser.startedAt).Milliseconds(), Hedge: loser.hedge, Superseded: true,
	})
	service.discardAttempt(requestCtx, requestID, loser)
}

// seedFirstEvents 从请求账本读取 firstEventSampleAge 内的全部记录补入首事件样本
func (manager *runtimeManager) seedFirstEvents(ctx context.Context) error {
	now := time.Now()
	page, err := manager.ledger.Records(ctx, api.UsageRecordQuery{From: now.Add(-firstEventSampleAge), To: now, Limit: 1 << 20})
	if err != nil {
		return err
	}
	manager.requests.firstEvents.seed(page.Items)
	return nil
}

// FirstEventLatencies 返回各模型与通道近期原请求首事件等待的 p95
func (manager *runtimeManager) FirstEventLatencies() []api.FirstEventLatency {
	return manager.requests.firstEvents.latencies(time.Now())
}
