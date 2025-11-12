package hidera

import (
	"maps"
	"math"
	"math/rand"
	"slices"
	"sync"
	"time"

	"github.com/tamararankovic/hidera/config"
	"github.com/tamararankovic/hidera/peers"
)

type Hidera struct {
	Params        config.Params
	Value         int
	Peers         *peers.Peers
	Round         int
	CountEstimate int
	LastMsg       map[string]int
	Trees         map[string]*Tree
	IsRoot        bool
	Lock          *sync.Mutex
}

func NewHidera(params config.Params, peers *peers.Peers) *Hidera {
	return &Hidera{
		Params:        params,
		Value:         1,
		Peers:         peers,
		Round:         0,
		CountEstimate: 1,
		LastMsg:       make(map[string]int),
		Trees:         make(map[string]*Tree),
		IsRoot:        false,
		Lock:          new(sync.Mutex),
	}
}

func (h *Hidera) Run() {
	go h.handlePeerAdded()
	go h.handleMessages()
	go func() {
		ticker := time.NewTicker(time.Duration(h.Params.Tagg) * time.Second)
		for range ticker.C {
			h.Lock.Lock()

			h.Round++
			h.removeInactiveTrees()
			h.removeFailedPeers()
			bestTree := h.findBestTree()
			for id, tree := range h.Trees {
				isBest := bestTree != nil && bestTree.ID == id
				tree.executeRound(Aggregate{
					Value: h.Value,
					Count: 1,
					Round: h.Round,
				}, isBest)
				if isBest || !tree.IsRoot {
					continue
				}
				delete(h.Trees, id)
				h.IsRoot = false
			}
			if len(h.Trees) == 0 {
				h.tryElectSelfAsRoot()
			} else {
				h.computeCount()
			}

			h.Lock.Unlock()
		}
	}()
}

func (h *Hidera) handleMessages() {
	for msgRcvd := range h.Peers.Messages {
		msgAny := BytesToMsg(msgRcvd.MsgBytes)
		if msgAny == nil {
			continue
		}
		h.LastMsg[msgRcvd.Sender.GetID()] = h.Round
		switch msgAny.Type() {
		case LOCAL_AGG_MSG_TYPE:
			msg := msgAny.(*LocalAggMsg)
			h.Lock.Lock()
			tree := h.getOrCreateTree(msg.TreeID, -1)
			bestTree := h.findBestTree()
			if tree == nil || (bestTree != nil && bestTree.ID != tree.ID) {
				h.Lock.Unlock()
				continue
			}
			tree.onLocalAggMsg(*msg, msgRcvd.Sender)
			h.Lock.Unlock()
		case GLOBAL_AGG_MSG_TYPE:
			msg := msgAny.(*GlobalAggMsg)
			h.Lock.Lock()
			tree := h.getOrCreateTree(msg.TreeID, msg.ValueRound)
			bestTree := h.findBestTree()
			if tree == nil || (bestTree != nil && bestTree.ID != tree.ID) {
				h.Lock.Unlock()
				continue
			}
			tree.onGlobalAggMsg(*msg, msgRcvd.Sender, h.Round)
			h.Lock.Unlock()
		case GLOBAL_AGG_LAZY_MSG_TYPE:
			msg := msgAny.(*GlobalAggLazyMsg)
			h.Lock.Lock()
			tree := h.getOrCreateTree(msg.TreeID, msg.ValueRound)
			bestTree := h.findBestTree()
			if tree == nil || (bestTree != nil && bestTree.ID != tree.ID) {
				h.Lock.Unlock()
				continue
			}
			tree.onGlobalLazyAggMsg(*msg, msgRcvd.Sender, h.Round)
			h.Lock.Unlock()
		}
	}
}

func (h *Hidera) handlePeerAdded() {
	for peer := range h.Peers.PeerAdded {
		h.Lock.Lock()
		for _, tree := range h.Trees {
			tree.addNewChild(peer)
		}
		h.Lock.Unlock()
	}
}

func (h *Hidera) removeInactiveTrees() {
	toRemove := make([]string, 0)
	for id, tree := range h.Trees {
		if (h.Round - tree.LastGlobalRound) > h.Params.Rmax {
			toRemove = append(toRemove, id)
		}
	}
	for _, id := range toRemove {
		delete(h.Trees, id)
	}
}

func (h *Hidera) removeFailedPeers() {
	for _, p := range h.Peers.GetPeers() {
		if h.Round-h.LastMsg[p.GetID()] <= h.Params.Rmax {
			continue
		}
		h.Peers.PeerFailed(p.GetID())
		for _, tree := range h.Trees {
			tree.removeChild(p)
			tree.removeLazy(p)
			if tree.Parent != nil && tree.Parent.GetID() == p.GetID() {
				tree.Parent = nil
			}
		}
	}
}

func (h *Hidera) tryElectSelfAsRoot() {
	go func() {
		for len(slices.Collect(maps.Keys(h.Trees))) == 0 {
			h.Lock.Lock()
			num := rand.Float64()
			if num <= (1 / math.Max(float64(h.CountEstimate), 1)) {
				tree := h.getOrCreateTree(h.Params.ID, h.Round)
				tree.LastGlobalRound = h.Round
				tree.IsRoot = true
				h.IsRoot = true
			}
			h.Lock.Unlock()
			time.Sleep(time.Duration(h.Params.Telect) * time.Second)
		}
	}()
}

func (h *Hidera) computeCount() {
	tree := h.findBestTree()
	if tree == nil || tree.GlobalAgg == nil || tree.GlobalAgg.Round-tree.FirstGlobalRound <= h.Params.Rfull {
		return
	}
	h.CountEstimate = tree.GlobalAgg.Count
}

func (h *Hidera) findBestTree() *Tree {
	var best *Tree
	maxId := ""
	for id, tree := range h.Trees {
		if id > maxId {
			best = tree
			maxId = id
		}
	}
	return best
}

func (h *Hidera) getOrCreateTree(id string, round int) *Tree {
	tree := h.Trees[id]
	if tree == nil {
		tree = NewTree(h.Params, id, round, h.Peers.GetPeers())
		h.Trees[id] = tree
		return tree
	}
	if tree.FirstGlobalRound > round {
		tree.FirstGlobalRound = round
	}
	return tree
}
