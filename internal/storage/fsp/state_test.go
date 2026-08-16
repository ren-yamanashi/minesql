package fsp

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtentStateString(t *testing.T) {
	t.Run("各状態の文字列表現", func(t *testing.T) {
		// THEN
		assert.Equal(t, "NOT_INITED", stateNotInited.String())
		assert.Equal(t, "FREE", stateFree.String())
		assert.Equal(t, "FREE_FRAG", stateFreeFrag.String())
		assert.Equal(t, "FULL_FRAG", stateFullFrag.String())
		assert.Equal(t, "extentState(9)", extentState(9).String())
	})
}

func TestAssertStateTransition(t *testing.T) {
	states := []extentState{stateNotInited, stateFree, stateFreeFrag, stateFullFrag}
	allowed := map[[2]extentState]bool{
		{stateNotInited, stateFree}:    true,
		{stateFree, stateFreeFrag}:     true,
		{stateFreeFrag, stateFullFrag}: true,
		{stateFullFrag, stateFreeFrag}: true,
		{stateFreeFrag, stateFree}:     true,
	}
	for _, from := range states {
		for _, to := range states {
			if allowed[[2]extentState{from, to}] {
				t.Run(fmt.Sprintf("%s から %s への遷移は許容される", from, to), func(t *testing.T) {
					// THEN
					assert.NotPanics(t, func() { assertStateTransition(from, to) })
				})
			} else {
				t.Run(fmt.Sprintf("%s から %s への遷移は panic する", from, to), func(t *testing.T) {
					// THEN
					assert.Panics(t, func() { assertStateTransition(from, to) })
				})
			}
		}
	}
}
