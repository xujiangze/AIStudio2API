package aistudio

import (
	"sort"
	"time"
)

const (
	// RoutingStrategyRoundRobin 按范围游标依次轮询账户
	RoutingStrategyRoundRobin = "round-robin"
	// RoutingStrategyFillFirst 按账户顺序优先填满首个可用账户
	RoutingStrategyFillFirst = "fill-first"
	// RoutingStrategyLeastRecent 按空闲优先、时间窗内近期请求最少、最久未使用均衡分配账户
	RoutingStrategyLeastRecent = "least-recent"

	// DefaultRoutingRecencyWindow 是近期均衡默认的时间窗口长度
	DefaultRoutingRecencyWindow = 5 * time.Minute

	maxRecencySamples = 4096
)

// SetRoutingRecencyWindow 设置近期均衡统计所用的滑动时间窗，0 表示不计历史次数
func (p *AccountPool) SetRoutingRecencyWindow(window time.Duration) {
	if window < 0 {
		window = 0
	}
	p.mu.Lock()
	p.recencyWindow = window
	p.mu.Unlock()
}

func (p *AccountPool) recencyRetentionLocked() time.Duration {
	if p.recencyWindow > DefaultRoutingRecencyWindow {
		return p.recencyWindow
	}
	return DefaultRoutingRecencyWindow
}

func (p *AccountPool) pruneRecentUsesLocked(account *Account, now time.Time) {
	if len(account.recentUses) == 0 {
		return
	}
	retention := p.recencyRetentionLocked()
	first := sort.Search(len(account.recentUses), func(i int) bool {
		return now.Sub(account.recentUses[i]) < retention
	})
	if first == len(account.recentUses) {
		account.recentUses = account.recentUses[:0]
		return
	}
	if first > 0 {
		remaining := len(account.recentUses) - first
		if first >= 64 || first > remaining {
			compacted := make([]time.Time, remaining)
			copy(compacted, account.recentUses[first:])
			account.recentUses = compacted
		} else {
			account.recentUses = account.recentUses[first:]
		}
	}
	if overflow := len(account.recentUses) - maxRecencySamples; overflow > 0 {
		compacted := make([]time.Time, maxRecencySamples)
		copy(compacted, account.recentUses[overflow:])
		account.recentUses = compacted
	}
}

func (p *AccountPool) recordRecentUseLocked(account *Account, now time.Time) {
	p.pruneRecentUsesLocked(account, now)
	if n := len(account.recentUses); n == 0 || !now.Before(account.recentUses[n-1]) {
		account.recentUses = append(account.recentUses, now)
	} else {
		pos := sort.Search(n, func(i int) bool { return account.recentUses[i].After(now) })
		account.recentUses = append(account.recentUses, time.Time{})
		copy(account.recentUses[pos+1:], account.recentUses[pos:])
		account.recentUses[pos] = now
	}
	if len(account.recentUses) > maxRecencySamples {
		overflow := len(account.recentUses) - maxRecencySamples
		compacted := make([]time.Time, maxRecencySamples)
		copy(compacted, account.recentUses[overflow:])
		account.recentUses = compacted
	}
}

func (p *AccountPool) recentUseCountLocked(account *Account, now time.Time) int {
	if p.recencyWindow <= 0 || len(account.recentUses) == 0 {
		return 0
	}
	p.pruneRecentUsesLocked(account, now)
	if len(account.recentUses) == 0 {
		return 0
	}
	first := sort.Search(len(account.recentUses), func(i int) bool {
		return now.Sub(account.recentUses[i]) < p.recencyWindow
	})
	return len(account.recentUses) - first
}

func (p *AccountPool) sortLeastRecentLocked(indices []int, now time.Time) {
	if len(indices) <= 1 {
		return
	}
	counts := make(map[int]int, len(indices))
	for _, index := range indices {
		counts[index] = p.recentUseCountLocked(p.accounts[index], now)
	}
	sort.Slice(indices, func(left, right int) bool {
		leftIndex, rightIndex := indices[left], indices[right]
		leftAccount, rightAccount := p.accounts[leftIndex], p.accounts[rightIndex]
		if leftAccount.active != rightAccount.active {
			return leftAccount.active < rightAccount.active
		}
		if counts[leftIndex] != counts[rightIndex] {
			return counts[leftIndex] < counts[rightIndex]
		}
		if leftAccount.lastRoutedAt.IsZero() != rightAccount.lastRoutedAt.IsZero() {
			return leftAccount.lastRoutedAt.IsZero()
		}
		if !leftAccount.lastRoutedAt.Equal(rightAccount.lastRoutedAt) {
			return leftAccount.lastRoutedAt.Before(rightAccount.lastRoutedAt)
		}
		if leftAccount.lastRoutedSeq != rightAccount.lastRoutedSeq {
			return leftAccount.lastRoutedSeq < rightAccount.lastRoutedSeq
		}
		return leftAccount.ID < rightAccount.ID
	})
}
