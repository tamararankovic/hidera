package hidera

import (
	"maps"
	"slices"

	"github.com/tamararankovic/hidera/peers"
)

type LagMetric struct {
	lagByPeer map[string]map[int]struct{}
}

func NewLagMetric(peers []peers.Peer) *LagMetric {
	lm := &LagMetric{
		lagByPeer: make(map[string]map[int]struct{}),
	}
	for _, p := range peers {
		lm.lagByPeer[p.GetID()] = make(map[int]struct{})
	}
	return lm
}

func (lm *LagMetric) Add(peerId string, round int) {
	lm.lagByPeer[peerId][round] = struct{}{}
}

func (lm *LagMetric) Get(peerId string) int {
	lag := lm.lagByPeer[peerId]
	if lag == nil {
		return 0
	}
	return len(slices.Collect(maps.Keys(lag)))
}

func (lm *LagMetric) Reset() {
	for peerId := range lm.lagByPeer {
		lm.lagByPeer[peerId] = make(map[int]struct{})
	}
}

func (lm *LagMetric) ForgetOlder(minRound int) {
	for peerId := range lm.lagByPeer {
		toRemove := make([]int, 0)
		for round := range lm.lagByPeer[peerId] {
			if round < minRound {
				toRemove = append(toRemove, round)
			}
		}
		for _, round := range toRemove {
			delete(lm.lagByPeer[peerId], round)
		}
	}
}
