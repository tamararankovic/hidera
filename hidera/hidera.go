package hidera

import (
	"log"
	"maps"
	"math"
	"math/rand"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/tamararankovic/hidera/config"
	"github.com/tamararankovic/hidera/peers"
)

type Hidera struct {
	Params        config.Params
	Value         float64
	Peers         *peers.Peers
	Round         int
	CountEstimate int
	LastMsg       map[string]int
	Trees         map[string]*Tree
	IsRoot        bool
	Lock          *sync.Mutex
	electing      bool
}

func NewHidera(params config.Params, peers *peers.Peers) *Hidera {
	log.Printf("[INIT] NewHidera created with ID=%s", params.ID)
	val, err := strconv.Atoi(params.ID)
	if err != nil {
		log.Fatal(err)
	}
	return &Hidera{
		Params:        params,
		Value:         float64(val),
		Peers:         peers,
		Round:         0,
		CountEstimate: 1,
		LastMsg:       make(map[string]int),
		Trees:         make(map[string]*Tree),
		IsRoot:        false,
		Lock:          new(sync.Mutex),
		electing:      false,
	}
}

func (h *Hidera) Run() {
	log.Printf("[START] Node %s starting Run()", h.Params.ID)

	go h.handlePeerAdded()
	go h.handleMessages()
	go h.sendPingToFailed()

	go func() {
		ticker := time.NewTicker(time.Duration(h.Params.Tagg) * time.Second)
		for range ticker.C {

			h.Lock.Lock()
			log.Printf("[ROUND] Node %s entering round %d", h.Params.ID, h.Round+1)

			h.Round++

			h.removeInactiveTrees()
			h.removeFailedPeers()

			bestTree := h.FindBestTree()
			if bestTree != nil {
				log.Printf("[BEST TREE] Node %s best tree = %s", h.Params.ID, bestTree.ID)
			}

			for id, tree := range h.Trees {
				isBest := bestTree != nil && bestTree.ID == id
				log.Printf("[EXEC ROUND] Node %s exec tree %s (isBest=%t)", h.Params.ID, id, isBest)

				tree.executeRound(Aggregate{
					Value: h.Value,
					Count: 1,
					Round: h.Round,
				}, isBest)

				if isBest || !tree.IsRoot {
					continue
				}

				log.Printf("[REMOVE ROOT] Node %s removing tree %s because it is not best", h.Params.ID, id)
				delete(h.Trees, id)
				h.IsRoot = false
			}

			if len(h.Trees) == 0 {
				h.sendPing()
				// wait some time to see if messages start arriving
				if !h.electing && h.Round > 5 {
					log.Printf("[NO TREES] Node %s tries to elect itself as root", h.Params.ID)
					h.electing = true
					h.tryElectSelfAsRoot()
				}
			} else {
				h.computeCount()
				log.Printf("[COUNT] Node %s CountEstimate updated to %d", h.Params.ID, h.CountEstimate)
			}

			h.Lock.Unlock()
		}
	}()
}

func (h *Hidera) handleMessages() {
	log.Printf("[MSG LOOP] Node %s starting handleMessages()", h.Params.ID)

	for msgRcvd := range h.Peers.Messages {
		msgAny := BytesToMsg(msgRcvd.MsgBytes)
		if msgAny == nil {
			log.Printf("[WARN] Node %s received nil/invalid message", h.Params.ID)
			continue
		}

		senderID := msgRcvd.Sender.GetID()
		h.LastMsg[senderID] = h.Round
		log.Printf("[MSG RECEIVED] Node %s got %T from %s at round %d",
			h.Params.ID, msgAny, senderID, h.Round)

		switch msgAny.Type() {
		case LOCAL_AGG_MSG_TYPE:
			msg := msgAny.(*LocalAggMsg)
			h.Lock.Lock()
			log.Printf("[LOCAL_AGG] Node %s tree=%s sender=%s", h.Params.ID, msg.TreeID, senderID)

			tree := h.getOrCreateTree(msg.TreeID, -1)
			bestTree := h.FindBestTree()

			if tree == nil || (bestTree != nil && bestTree.ID != tree.ID) {
				log.Printf("[LOCAL_AGG] Node %s dropped (not best tree)", h.Params.ID)
				h.Lock.Unlock()
				continue
			}

			tree.onLocalAggMsg(*msg, msgRcvd.Sender)
			h.Lock.Unlock()

		case GLOBAL_AGG_MSG_TYPE:
			msg := msgAny.(*GlobalAggMsg)
			h.Lock.Lock()
			log.Printf("[GLOBAL_AGG] Node %s tree=%s sender=%s", h.Params.ID, msg.TreeID, senderID)

			tree := h.getOrCreateTree(msg.TreeID, msg.ValueRound)
			bestTree := h.FindBestTree()

			if tree == nil || (bestTree != nil && bestTree.ID != tree.ID) {
				log.Printf("[GLOBAL_AGG] Node %s dropped (not best tree)", h.Params.ID)
				h.Lock.Unlock()
				continue
			}

			tree.onGlobalAggMsg(*msg, msgRcvd.Sender, h.Round)
			h.Lock.Unlock()

		case GLOBAL_AGG_LAZY_MSG_TYPE:
			msg := msgAny.(*GlobalAggLazyMsg)
			h.Lock.Lock()
			log.Printf("[GLOBAL_LAZY] Node %s tree=%s sender=%s", h.Params.ID, msg.TreeID, senderID)

			tree := h.getOrCreateTree(msg.TreeID, msg.ValueRound)
			bestTree := h.FindBestTree()

			if tree == nil || (bestTree != nil && bestTree.ID != tree.ID) {
				log.Printf("[GLOBAL_LAZY] Node %s dropped (not best tree)", h.Params.ID)
				h.Lock.Unlock()
				continue
			}

			tree.onGlobalLazyAggMsg(*msg, msgRcvd.Sender, h.Round)
			h.Lock.Unlock()
		}
	}
}

func (h *Hidera) handlePeerAdded() {
	log.Printf("[PEER_ADDED_LOOP] Node %s listening for PeerAdded", h.Params.ID)

	for peer := range h.Peers.PeerAdded {
		log.Printf("[PEER_ADDED] Node %s new peer=%s", h.Params.ID, peer.GetID())

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
		level := 1
		if tree.Level > 0 {
			level = tree.Level
		}
		if (h.Round - tree.LastGlobalRound) > level*h.Params.Rmax {
			log.Printf("[TREE_EXPIRED] Node %s removing inactive tree %s", h.Params.ID, id)
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
		// sequential statup of nodes
		if h.Round < 10 {
			continue
		}
		log.Printf("[PEER_FAIL] Node %s peer failed=%s", h.Params.ID, p.GetID())

		h.Peers.PeerFailed(p.GetID())
		delete(h.LastMsg, p.GetID())

		for _, tree := range h.Trees {
			tree.removeChild(p)
			tree.removeLazy(p)

			if tree.Parent != nil && tree.Parent.GetID() == p.GetID() {
				log.Printf("[REMOVE_PARENT] Node %s tree %s lost parent %s",
					h.Params.ID, tree.ID, p.GetID())
				tree.Parent = nil
			}

			delete(tree.LastRound, p.GetID())
			delete(tree.LocalAggs, p.GetID())
		}
	}
}

func (h *Hidera) tryElectSelfAsRoot() {
	log.Printf("[ELECTION_START] Node %s begins election loop", h.Params.ID)

	go func() {
		for len(slices.Collect(maps.Keys(h.Trees))) == 0 {

			h.Lock.Lock()
			num := rand.Float64()

			log.Printf("[ELECTION] Node %s rand=%f threshold=%f", h.Params.ID, num,
				1/math.Max(float64(h.CountEstimate), 1))

			if num <= (1 / math.Max(float64(h.CountEstimate), 1)) {
				tree := h.getOrCreateTree(h.Params.ID, h.Round)
				tree.LastGlobalRound = h.Round
				tree.IsRoot = true
				h.IsRoot = true

				log.Printf("[BECAME_ROOT] Node %s became root of tree %s", h.Params.ID, tree.ID)
			}

			h.Lock.Unlock()
			time.Sleep(time.Duration(h.Params.Telect) * time.Second)
		}
		h.Lock.Lock()
		h.electing = false
		h.Lock.Unlock()
	}()
}

func (h *Hidera) computeCount() {
	tree := h.FindBestTree()
	if tree == nil || tree.GlobalAgg == nil || tree.GlobalAgg.Round-tree.FirstGlobalRound <= h.Params.Rfull {
		return
	}
	log.Println("best tree", tree.ID)
	h.CountEstimate = tree.GlobalAgg.Count
	log.Printf("[COUNT_COMPUTED] Node %s new count = %d", h.Params.ID, h.CountEstimate)
}

func (h *Hidera) FindBestTree() *Tree {
	var best *Tree
	maxId := ""

	for id, tree := range h.Trees {
		if id > maxId {
			best = tree
			maxId = id
		}
	}
	if best != nil {
		log.Printf("[BEST] Node %s best tree=%s", h.Params.ID, best.ID)
	}
	return best
}

func (h *Hidera) getOrCreateTree(id string, round int) *Tree {
	tree := h.Trees[id]

	if tree == nil {
		log.Printf("[TREE_CREATE] Node %s creating tree %s (round=%d)", h.Params.ID, id, round)
		tree = NewTree(h.Params, id, round, h.Peers.GetPeers())
		h.Trees[id] = tree
		return tree
	}

	if tree.FirstGlobalRound > round {
		log.Printf("[TREE_UPDATE] Node %s tree %s FirstGlobalRound updated %d -> %d",
			h.Params.ID, id, tree.FirstGlobalRound, round)
		tree.FirstGlobalRound = round
	}
	return tree
}

func (h *Hidera) sendPing() {
	msg := []byte{byte(PING_MSG_TYPE)}
	for _, peer := range h.Peers.GetPeers() {
		peer.Send(msg)
	}
}

func (h *Hidera) sendPingToFailed() {
	msg := []byte{byte(PING_MSG_TYPE)}
	for range time.NewTicker(10 * time.Duration(h.Params.Tagg) * time.Second).C {
		h.Lock.Lock()
		failed := h.Peers.GetFailedPeers()
		h.Lock.Unlock()
		for _, peer := range failed {
			peer.Send(msg)
		}
	}
}
