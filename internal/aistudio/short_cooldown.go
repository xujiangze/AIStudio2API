package aistudio

import (
	"fmt"
	"maps"
	"strings"
	"time"
)

// ShortCooldownKind 表示独立保留的短冷却原因
type ShortCooldownKind string

const (
	// ShortCooldownRace 表示原请求在首字竞速中被不同账户的备用请求取代
	ShortCooldownRace ShortCooldownKind = "race"
	// ShortCooldownBusy 表示上游模型暂时繁忙
	ShortCooldownBusy ShortCooldownKind = "busy"
)

// ShortCooldownState 保存短冷却的首次标记时间、恢复时间与原因
type ShortCooldownState struct {
	MarkedAt time.Time `json:"marked_at"`
	Until    time.Time `json:"until"`
	Reason   string    `json:"reason"`
}

// ValidShortCooldownKind 判断 kind 是否为已知的短冷却类型
func ValidShortCooldownKind(kind ShortCooldownKind) bool {
	return kind == ShortCooldownRace || kind == ShortCooldownBusy
}

// ShortCooldownScope 返回明确包含通道与模型的短冷却键，空通道按 Playground 处理，无效参数返回空键
func ShortCooldownScope(channel Channel, modelID string) string {
	if channel == "" {
		channel = ChannelPlayground
	}
	modelID = strings.TrimPrefix(strings.TrimSpace(modelID), "models/")
	if !validShortCooldownChannel(channel) || modelID == "" {
		return ""
	}
	return string(channel) + ":" + modelID
}

// ParseShortCooldownScope 解析短冷却的通道与模型，无效作用域返回空通道与空模型
func ParseShortCooldownScope(scope string) (Channel, string) {
	channel, modelID, found := strings.Cut(strings.TrimSpace(scope), ":")
	if !found || !validShortCooldownChannel(Channel(channel)) {
		return "", ""
	}
	modelID = strings.TrimPrefix(strings.TrimSpace(modelID), "models/")
	if modelID == "" {
		return "", ""
	}
	return Channel(channel), modelID
}

func validShortCooldownChannel(channel Channel) bool {
	return channel == ChannelPlayground || channel == ChannelBuild || channel == ChannelAssistant
}

// MarkShortCooldown 保存账户、通道与模型上的指定短冷却，连续有效的同类型冷却保留最早标记时间、不缩短期限，时长为 0 时不新增冷却
func (p *AccountPool) MarkShortCooldown(
	accountID string,
	channel Channel,
	modelID string,
	kind ShortCooldownKind,
	now time.Time,
	duration time.Duration,
	reason string,
) (time.Time, error) {
	if !ValidShortCooldownKind(kind) {
		return time.Time{}, fmt.Errorf("无效的短冷却类型: %s", kind)
	}
	scope := ShortCooldownScope(channel, modelID)
	if scope == "" {
		return time.Time{}, fmt.Errorf("短冷却通道或模型不能为空或无效")
	}
	if duration < 0 {
		return time.Time{}, fmt.Errorf("短冷却时长不能为负数")
	}
	if duration == 0 {
		return time.Time{}, nil
	}
	if now.IsZero() {
		return time.Time{}, fmt.Errorf("短冷却标记时间不能为空")
	}
	now = now.UTC()
	reason = strings.TrimSpace(reason)
	var until time.Time
	_, err := p.updateRuntime(accountID, func(_ *Account, runtimeState *accountRuntimeState) (bool, func(*Account), error) {
		if runtimeState.ShortCooldowns[scope] == nil {
			runtimeState.ShortCooldowns[scope] = make(map[ShortCooldownKind]ShortCooldownState)
		}
		previous, exists := runtimeState.ShortCooldowns[scope][kind]
		next := ShortCooldownState{MarkedAt: now, Until: now.Add(duration), Reason: reason}
		if exists && previous.Until.After(now) {
			if previous.Until.After(next.Until) {
				next.Until = previous.Until
			}
			if previous.MarkedAt.Before(next.MarkedAt) {
				next.MarkedAt = previous.MarkedAt
			}
		}
		until = next.Until
		runtimeState.ShortCooldowns[scope][kind] = next
		return !exists || next != previous, nil, nil
	})
	return until, err
}

// ClearShortCooldown 手动清除指定作用域的一类短冷却，不影响其他短项或额度冷却
func (p *AccountPool) ClearShortCooldown(accountID, scope string, kind ShortCooldownKind) error {
	if !ValidShortCooldownKind(kind) {
		return fmt.Errorf("无效的短冷却类型: %s", kind)
	}
	channel, modelID := ParseShortCooldownScope(scope)
	if channel == "" {
		return fmt.Errorf("无效的短冷却作用域: %s", scope)
	}
	scope = ShortCooldownScope(channel, modelID)
	_, err := p.updateRuntime(accountID, func(_ *Account, runtimeState *accountRuntimeState) (bool, func(*Account), error) {
		kinds := runtimeState.ShortCooldowns[scope]
		if _, exists := kinds[kind]; !exists {
			return false, nil, nil
		}
		delete(kinds, kind)
		if len(kinds) == 0 {
			delete(runtimeState.ShortCooldowns, scope)
		}
		return true, nil, nil
	})
	return err
}

// channelSelectionCooldown 合并对选择生效的旧冷却与短项，只有取得租约早于标记时间的请求豁免短项
func channelSelectionCooldown(account *Account, channel Channel, selection AccountSelection, now, checkedAt time.Time) (CooldownState, bool) {
	selected, _ := selectionCooldown(account, ChannelCooldownScope(channel, selectionAccessScope(selection)), selection, now)
	scope := ShortCooldownScope(channel, selection.ModelID)
	for _, kind := range []ShortCooldownKind{ShortCooldownRace, ShortCooldownBusy} {
		cooldown, exists := account.runtime.ShortCooldowns[scope][kind]
		if !exists || !cooldown.Until.After(now) || !checkedAt.IsZero() && checkedAt.Before(cooldown.MarkedAt) {
			continue
		}
		if cooldown.Until.After(selected.Until) {
			selected = CooldownState{Until: cooldown.Until, Reason: cooldown.Reason}
		}
	}
	return selected, !selected.Until.IsZero()
}

func cloneShortCooldowns(cooldowns map[string]map[ShortCooldownKind]ShortCooldownState) map[string]map[ShortCooldownKind]ShortCooldownState {
	result := make(map[string]map[ShortCooldownKind]ShortCooldownState, len(cooldowns))
	for scope, kinds := range cooldowns {
		result[scope] = maps.Clone(kinds)
	}
	return result
}
