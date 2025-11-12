package hidera

import (
	"encoding/json"
)

const LOCAL_AGG_MSG_TYPE int8 = 1
const GLOBAL_AGG_MSG_TYPE int8 = 2
const GLOBAL_AGG_LAZY_MSG_TYPE int8 = 3

type Msg interface {
	Type() int8
}

type LocalAggMsg struct {
	TreeID      string
	Value       int
	Count       int
	SenderRound int
}

func (m LocalAggMsg) Type() int8 {
	return LOCAL_AGG_MSG_TYPE
}

type GlobalAggMsg struct {
	TreeID      string
	Value       int
	Count       int
	ValueRound  int
	SenderRound int
}

func (m GlobalAggMsg) Type() int8 {
	return GLOBAL_AGG_MSG_TYPE
}

type GlobalAggLazyMsg struct {
	TreeID      string
	ValueRound  int
	SenderRound int
}

func (m GlobalAggLazyMsg) Type() int8 {
	return GLOBAL_AGG_LAZY_MSG_TYPE
}

func MsgToBytes(msg Msg) []byte {
	msgBytes, _ := json.Marshal(&msg)
	return append([]byte{byte(msg.Type())}, msgBytes...)
}

func BytesToMsg(msgBytes []byte) Msg {
	msgType := int8(msgBytes[0])
	var msg Msg
	switch msgType {
	case LOCAL_AGG_MSG_TYPE:
		msg = &LocalAggMsg{}
	case GLOBAL_AGG_MSG_TYPE:
		msg = &GlobalAggMsg{}
	case GLOBAL_AGG_LAZY_MSG_TYPE:
		msg = &GlobalAggLazyMsg{}
	}
	if msg == nil {
		return nil
	}
	json.Unmarshal(msgBytes[1:], msg)
	return msg
}
