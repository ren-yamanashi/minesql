package fsp

import "fmt"

// extentState は extent の状態
type extentState uint32

const (
	stateNotInited extentState = 0
	stateFree      extentState = 1
	stateFreeFrag  extentState = 2
	stateFullFrag  extentState = 3
)

func (s extentState) String() string {
	switch s {
	case stateNotInited:
		return "NOT_INITED"
	case stateFree:
		return "FREE"
	case stateFreeFrag:
		return "FREE_FRAG"
	case stateFullFrag:
		return "FULL_FRAG"
	default:
		return fmt.Sprintf("extentState(%d)", uint32(s))
	}
}

// assertStateTransition は from から to への遷移が許容されない場合に panic する
func assertStateTransition(from, to extentState) {
	if isAllowedStateTransition(from, to) {
		return
	}
	panic(fmt.Sprintf("fsp: invalid extent state transition: %s -> %s", from, to))
}

func isAllowedStateTransition(from, to extentState) bool {
	switch {
	case from == stateNotInited && to == stateFree,
		from == stateFree && to == stateFreeFrag,
		from == stateFreeFrag && to == stateFullFrag,
		from == stateFullFrag && to == stateFreeFrag,
		from == stateFreeFrag && to == stateFree:
		return true
	default:
		return false
	}
}
