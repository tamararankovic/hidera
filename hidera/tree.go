package hidera

import (
	"log"
	"maps"
	"slices"

	"github.com/tamararankovic/hidera/config"
	"github.com/tamararankovic/hidera/peers"
)

type Tree struct {
	Params           config.Params
	ID               string
	FirstGlobalRound int
	LastGlobalRound  int
	Parent           *peers.Peer
	Children         []peers.Peer
	Lazy             []peers.Peer
	LastRound        map[string]int
	ParentLag        *LagMetric
	IsRoot           bool
	LocalAggs        map[string]Aggregate
	GlobalAgg        *Aggregate
}

func NewTree(params config.Params, id string, round int, ps []peers.Peer) *Tree {
	return &Tree{
		Params:           params,
		ID:               id,
		FirstGlobalRound: round,
		LastGlobalRound:  -1,
		Parent:           nil,
		Children:         slices.Clone(ps),
		Lazy:             make([]peers.Peer, 0),
		LastRound:        make(map[string]int),
		ParentLag:        NewLagMetric(ps),
		IsRoot:           false,
		LocalAggs:        map[string]Aggregate{},
		GlobalAgg:        nil,
	}
}

func (t *Tree) executeRound(currLocal Aggregate, bestTree bool) {
	t.ParentLag.ForgetOlder(currLocal.Round - t.Params.Rwindow)
	t.findBetterParent()
	if !bestTree {
		return
	}
	t.sendGlobalAgg(currLocal)
	if t.Parent != nil {
		t.sendLocalAgg(currLocal)
	}
}

func (t *Tree) sendGlobalAgg(currLocal Aggregate) {
	if t.IsRoot {
		ga := currLocal.Aggregate(slices.Collect(maps.Values(t.LocalAggs)))
		t.GlobalAgg = &ga
		t.LastGlobalRound = ga.Round
	}
	if t.GlobalAgg == nil {
		return
	}
	globalAggMsg := MsgToBytes(GlobalAggMsg{
		TreeID:      t.ID,
		Value:       t.GlobalAgg.Value,
		Count:       t.GlobalAgg.Count,
		ValueRound:  t.GlobalAgg.Round,
		SenderRound: currLocal.Round,
	})
	for _, p := range t.Children {
		p.Send(globalAggMsg)
	}
	globalAggLazyMsg := MsgToBytes(GlobalAggLazyMsg{
		TreeID:      t.ID,
		ValueRound:  t.GlobalAgg.Round,
		SenderRound: currLocal.Round,
	})
	for _, p := range t.Lazy {
		p.Send(globalAggLazyMsg)
	}
}

func (t *Tree) sendLocalAgg(currLocal Aggregate) {
	localAgg := currLocal.Aggregate(slices.Collect(maps.Values(t.LocalAggs)))
	msg := MsgToBytes(LocalAggMsg{
		TreeID:      t.ID,
		Value:       localAgg.Value,
		Count:       localAgg.Count,
		SenderRound: localAgg.Round,
	})
	t.Parent.Send(msg)
}

func (t *Tree) onLocalAggMsg(msg LocalAggMsg, sender peers.Peer) {
	if t.LastRound[sender.GetID()] > msg.SenderRound {
		return
	}
	t.LastRound[sender.GetID()] = msg.SenderRound
	t.updateRelationship(sender, LOCAL_AGG_MSG_TYPE)
	if !t.isChild(sender) {
		return
	}
	if msg.SenderRound > t.LocalAggs[sender.GetID()].Round {
		t.LocalAggs[sender.GetID()] = Aggregate{
			Value: msg.Value,
			Count: msg.Count,
			Round: msg.SenderRound,
		}
	}
}

func (t *Tree) onGlobalAggMsg(msg GlobalAggMsg, sender peers.Peer, localRound int) {
	if t.LastRound[sender.GetID()] > msg.SenderRound {
		return
	}
	t.LastRound[sender.GetID()] = msg.SenderRound
	t.updateRelationship(sender, GLOBAL_AGG_MSG_TYPE)
	if !t.isParent(sender) {
		return
	}
	if t.GlobalAgg == nil || msg.ValueRound > t.GlobalAgg.Round {
		t.GlobalAgg = &Aggregate{
			Value: msg.Value,
			Count: msg.Count,
			Round: msg.ValueRound,
		}
		t.LastGlobalRound = localRound
		log.Println("new global aggregate", "rootID =", t.ID, "value =", t.GlobalAgg.Value, "count =", t.GlobalAgg.Count)
	}
}

func (t *Tree) onGlobalLazyAggMsg(msg GlobalAggLazyMsg, sender peers.Peer, localRound int) {
	if t.LastRound[sender.GetID()] > msg.SenderRound {
		return
	}
	t.LastRound[sender.GetID()] = msg.SenderRound
	t.updateRelationship(sender, GLOBAL_AGG_LAZY_MSG_TYPE)
	if !t.isLazy(sender) {
		return
	}
	if t.GlobalAgg == nil || msg.ValueRound > t.GlobalAgg.Round {
		t.LastGlobalRound = localRound
		t.ParentLag.Add(sender.GetID(), localRound)
	}
}

func (t *Tree) updateRelationship(peer peers.Peer, msgType int8) {
	if msgType == LOCAL_AGG_MSG_TYPE {
		if t.isLazy(peer) {
			t.removeLazy(peer)
			t.addNewChild(peer)
		} else if t.isParent(peer) && peer.GetID() > t.Params.ID {
			t.Parent = nil
			t.addNewChild(peer)
			t.findBetterParent()
		}
	} else if msgType == GLOBAL_AGG_MSG_TYPE {
		if t.Parent == nil {
			t.removeChild(peer)
			t.removeLazy(peer)
			t.Parent = &peer
			t.ParentLag.Reset()
		} else if t.isChild(peer) {
			t.removeChild(peer)
			t.addNewLazy(peer)
		}
	} else if msgType == GLOBAL_AGG_LAZY_MSG_TYPE {
		if t.isChild(peer) {
			t.removeChild(peer)
			t.addNewLazy(peer)
		}
	}
}

func (t *Tree) findBetterParent() {
	candidate, lag := t.findBestParentCandidate()
	if candidate == nil || (t.Parent != nil && lag <= t.Params.Threshold) {
		return
	}
	t.removeLazy(*candidate)
	t.Parent = candidate
	t.ParentLag.Reset()
}

func (t *Tree) findBestParentCandidate() (*peers.Peer, int) {
	var candidate *peers.Peer
	maxLag := 0
	for _, p := range t.Lazy {
		currLag := t.ParentLag.Get(p.GetID())
		if currLag > maxLag {
			candidate = &p
			maxLag = currLag
		}
	}
	return candidate, maxLag
}

func (t *Tree) addNewChild(child peers.Peer) {
	if t.isParent(child) || t.isChild(child) || t.isLazy(child) {
		return
	}
	t.Children = append(t.Children, child)
}

func (t *Tree) addNewLazy(lazy peers.Peer) {
	if t.isParent(lazy) || t.isChild(lazy) || t.isLazy(lazy) {
		return
	}
	t.Children = append(t.Lazy, lazy)
}

func (t *Tree) removeChild(child peers.Peer) {
	t.Children = slices.DeleteFunc(t.Children, func(p peers.Peer) bool {
		return p.GetID() == child.GetID()
	})
}

func (t *Tree) removeLazy(lazy peers.Peer) {
	t.Lazy = slices.DeleteFunc(t.Lazy, func(p peers.Peer) bool {
		return p.GetID() == lazy.GetID()
	})
}

func (t *Tree) isParent(peer peers.Peer) bool {
	return t.Parent != nil && t.Parent.GetID() == peer.GetID()
}

func (t *Tree) isChild(peer peers.Peer) bool {
	return slices.ContainsFunc(t.Children, func(p peers.Peer) bool {
		return peer.GetID() == p.GetID()
	})
}

func (t *Tree) isLazy(peer peers.Peer) bool {
	return slices.ContainsFunc(t.Lazy, func(p peers.Peer) bool {
		return peer.GetID() == p.GetID()
	})
}
