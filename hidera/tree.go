package hidera

import (
	"log"
	"maps"
	"slices"

	"github.com/tamararankovic/hidera/config"
	"github.com/tamararankovic/hidera/peers"
)

type Tree struct {
	Params            config.Params
	ID                string
	FirstGlobalRound  int
	LastGlobalRound   int
	Parent            *peers.Peer
	Children          []peers.Peer
	Lazy              []peers.Peer
	LastRound         map[string]int
	ParentLag         *LagMetric
	IsRoot            bool
	LocalAggs         map[string]Aggregate
	GlobalAgg         *Aggregate
	ParentLastChanged int
	CurrRound         int
}

func NewTree(params config.Params, id string, round int, ps []peers.Peer) *Tree {
	log.Printf("[TREE CREATE] New tree id=%s round=%d children=%d", id, round, len(ps))
	return &Tree{
		Params:            params,
		ID:                id,
		FirstGlobalRound:  round,
		LastGlobalRound:   -1,
		Parent:            nil,
		Children:          slices.Clone(ps),
		Lazy:              make([]peers.Peer, 0),
		LastRound:         make(map[string]int),
		ParentLag:         NewLagMetric(ps),
		IsRoot:            false,
		LocalAggs:         map[string]Aggregate{},
		GlobalAgg:         nil,
		ParentLastChanged: -1,
		CurrRound:         -1,
	}
}

func (t *Tree) executeRound(currLocal Aggregate, bestTree bool) {
	log.Printf("[EXEC ROUND] tree=%s isRoot=%t bestTree=%t localValue=%d localRound=%d",
		t.ID, t.IsRoot, bestTree, currLocal.Value, currLocal.Round)

	t.CurrRound = currLocal.Round
	t.ParentLag.ForgetOlder(t.CurrRound - t.Params.Rwindow)
	if t.CurrRound-t.ParentLastChanged > t.Params.Rwindow {
		t.findBetterParent()
	}

	if !bestTree {
		log.Printf("[EXEC ROUND] tree=%s is not best tree → skipping root actions", t.ID)
		return
	}

	t.sendGlobalAgg(currLocal)

	if t.Parent != nil {
		t.sendLocalAgg(currLocal)
	}
}

func (t *Tree) sendGlobalAgg(currLocal Aggregate) {
	if t.IsRoot {
		log.Printf("[SEND GLOBAL AGG] tree=%s computing new GlobalAgg", t.ID)
		ga := currLocal.Aggregate(slices.Collect(maps.Values(t.LocalAggs)))
		t.GlobalAgg = &ga
		t.LastGlobalRound = ga.Round
	}

	if t.GlobalAgg == nil {
		log.Printf("[SEND GLOBAL AGG] tree=%s NO GlobalAgg yet (not root?)", t.ID)
		return
	}

	log.Printf("[SEND GLOBAL AGG] tree=%s sending to %d children", t.ID, len(t.Children))

	globalAggMsg := MsgToBytes(GlobalAggMsg{
		TreeID:      t.ID,
		Value:       t.GlobalAgg.Value,
		Count:       t.GlobalAgg.Count,
		ValueRound:  t.GlobalAgg.Round,
		SenderRound: t.CurrRound,
	})
	for _, p := range t.Children {
		log.Printf("[SEND GLOBAL_AGG] tree=%s → child=%s", t.ID, p.GetID())
		p.Send(globalAggMsg)
	}

	globalAggLazyMsg := MsgToBytes(GlobalAggLazyMsg{
		TreeID:      t.ID,
		ValueRound:  t.GlobalAgg.Round,
		SenderRound: t.CurrRound,
	})
	for _, p := range t.Lazy {
		log.Printf("[SEND GLOBAL_AGG_LAZY] tree=%s → lazy=%s", t.ID, p.GetID())
		p.Send(globalAggLazyMsg)
	}
}

func (t *Tree) sendLocalAgg(currLocal Aggregate) {
	log.Printf("[SEND LOCAL AGG] tree=%s to parent=%s", t.ID, t.Parent.GetID())

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
	log.Printf("[RCV LOCAL_AGG] tree=%s from=%s value=%d round=%d", t.ID, sender.GetID(), msg.Value, msg.SenderRound)

	if t.LastRound[sender.GetID()] > msg.SenderRound {
		log.Printf("[RCV LOCAL_AGG] tree=%s from=%s DROPPED (old round)", t.ID, sender.GetID())
		return
	}

	t.LastRound[sender.GetID()] = msg.SenderRound
	t.updateRelationship(sender, LOCAL_AGG_MSG_TYPE)

	if !t.isChild(sender) {
		log.Printf("[LOCAL_AGG] tree=%s sender=%s is not child → ignore content", t.ID, sender.GetID())
		return
	}

	if msg.SenderRound > t.LocalAggs[sender.GetID()].Round {
		log.Printf("[LOCAL_AGG] tree=%s updating LocalAgg from child=%s", t.ID, sender.GetID())
		t.LocalAggs[sender.GetID()] = Aggregate{
			Value: msg.Value,
			Count: msg.Count,
			Round: msg.SenderRound,
		}
	}
}

func (t *Tree) onGlobalAggMsg(msg GlobalAggMsg, sender peers.Peer, localRound int) {
	log.Printf("[RCV GLOBAL_AGG] tree=%s from=%s value=%d count=%d vr=%d",
		t.ID, sender.GetID(), msg.Value, msg.Count, msg.ValueRound)

	if t.LastRound[sender.GetID()] > msg.SenderRound {
		log.Printf("[RCV GLOBAL_AGG] tree=%s from=%s DROPPED (old round)", t.ID, sender.GetID())
		return
	}

	t.LastRound[sender.GetID()] = msg.SenderRound
	t.updateRelationship(sender, GLOBAL_AGG_MSG_TYPE)

	if !t.isParent(sender) {
		log.Printf("[RCV GLOBAL_AGG] tree=%s sender=%s is not parent → ignore", t.ID, sender.GetID())
		return
	}

	if t.GlobalAgg == nil || msg.ValueRound > t.GlobalAgg.Round {
		log.Printf("[GLOBAL_AGG UPDATE] tree=%s new global agg value=%d count=%d",
			t.ID, msg.Value, msg.Count)
		t.GlobalAgg = &Aggregate{
			Value: msg.Value,
			Count: msg.Count,
			Round: msg.ValueRound,
		}
		t.LastGlobalRound = localRound
	}
}

func (t *Tree) onGlobalLazyAggMsg(msg GlobalAggLazyMsg, sender peers.Peer, localRound int) {
	log.Printf("[RCV GLOBAL_LAZY] tree=%s from=%s vr=%d", t.ID, sender.GetID(), msg.ValueRound)

	if t.LastRound[sender.GetID()] > msg.SenderRound {
		log.Printf("[RCV GLOBAL_LAZY] tree=%s from=%s DROPPED (old round)", t.ID, sender.GetID())
		return
	}

	t.LastRound[sender.GetID()] = msg.SenderRound
	t.updateRelationship(sender, GLOBAL_AGG_LAZY_MSG_TYPE)

	if !t.isLazy(sender) {
		log.Printf("[RCV GLOBAL_LAZY] tree=%s sender=%s is not lazy → ignore", t.ID, sender.GetID())
		return
	}

	if t.GlobalAgg == nil || msg.ValueRound > t.GlobalAgg.Round {
		log.Printf("[GLOBAL_LAZY UPDATE] tree=%s updating ParentLag from lazy=%s", t.ID, sender.GetID())
		t.LastGlobalRound = localRound
		t.ParentLag.Add(sender.GetID(), localRound)
	}
}

func (t *Tree) updateRelationship(peer peers.Peer, msgType int8) {
	log.Printf("[REL UPDATE] tree=%s from=%s msgType=%d", t.ID, peer.GetID(), msgType)

	if msgType == LOCAL_AGG_MSG_TYPE {
		if t.isLazy(peer) {
			log.Printf("[REL] tree=%s lazy→child %s", t.ID, peer.GetID())
			t.removeLazy(peer)
			t.addNewChild(peer)
		} else if t.isParent(peer) && peer.GetID() > t.Params.ID {
			log.Printf("[REL] tree=%s replacing parent with child %s", t.ID, peer.GetID())
			t.Parent = nil
			t.addNewChild(peer)
			t.findBetterParent()
		}

	} else if msgType == GLOBAL_AGG_MSG_TYPE {
		if t.Parent == nil && !t.IsRoot {
			log.Printf("[REL] tree=%s setting new parent=%s", t.ID, peer.GetID())
			t.removeChild(peer)
			t.removeLazy(peer)
			t.Parent = &peer
			t.ParentLag.Reset()

		} else if t.isChild(peer) {
			log.Printf("[REL] tree=%s child→lazy %s", t.ID, peer.GetID())
			t.removeChild(peer)
			t.addNewLazy(peer)
		}

	} else if msgType == GLOBAL_AGG_LAZY_MSG_TYPE {
		if t.isChild(peer) {
			log.Printf("[REL] tree=%s child→lazy via lazy message %s", t.ID, peer.GetID())
			t.removeChild(peer)
			t.addNewLazy(peer)
		}
	}
}

func (t *Tree) findBetterParent() {
	if t.IsRoot {
		return
	}
	candidate, lag := t.findBestParentCandidate()
	if candidate == nil {
		return
	}

	if t.Parent != nil && lag <= t.Params.Threshold {
		log.Printf("[PARENT CHECK] tree=%s candidate=%s lag=%d NOT better (threshold=%d)",
			t.ID, candidate.GetID(), lag, t.Params.Threshold)
		return
	}

	log.Printf("[PARENT CHANGE] tree=%s new parent=%s lag=%d", t.ID, candidate.GetID(), lag)
	t.removeLazy(*candidate)
	t.Parent = candidate
	t.ParentLag.Reset()
	t.ParentLastChanged = t.CurrRound
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

	if candidate != nil {
		log.Printf("[BEST PARENT] tree=%s bestCandidate=%s lag=%d", t.ID, candidate.GetID(), maxLag)
	}
	return candidate, maxLag
}

func (t *Tree) addNewChild(child peers.Peer) {
	log.Printf("[CHILD ADD] tree=%s child=%s", t.ID, child.GetID())

	if t.isParent(child) || t.isChild(child) || t.isLazy(child) {
		log.Printf("[CHILD ADD] tree=%s child=%s skipped (already present)", t.ID, child.GetID())
		return
	}
	t.Children = append(t.Children, child)
}

func (t *Tree) addNewLazy(lazy peers.Peer) {
	log.Printf("[LAZY ADD] tree=%s lazy=%s", t.ID, lazy.GetID())

	if t.isParent(lazy) || t.isChild(lazy) || t.isLazy(lazy) {
		log.Printf("[LAZY ADD] tree=%s lazy=%s skipped (already present)", t.ID, lazy.GetID())
		return
	}

	t.Lazy = append(t.Lazy, lazy)
}

func (t *Tree) removeChild(child peers.Peer) {
	log.Printf("[CHILD REMOVE] tree=%s child=%s", t.ID, child.GetID())

	t.Children = slices.DeleteFunc(t.Children, func(p peers.Peer) bool {
		return p.GetID() == child.GetID()
	})
}

func (t *Tree) removeLazy(lazy peers.Peer) {
	log.Printf("[LAZY REMOVE] tree=%s lazy=%s", t.ID, lazy.GetID())

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
