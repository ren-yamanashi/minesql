package server

import (
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestShutdown_Add(t *testing.T) {
	t.Run("フックを追加できる", func(t *testing.T) {
		// GIVEN
		s := NewShutdown()

		// WHEN
		err := s.Add("hook1", func(sig os.Signal) {})

		// THEN
		assert.NoError(t, err)
		assert.Equal(t, 1, len(s.getHooks()))
	})

	t.Run("複数のフックを追加できる", func(t *testing.T) {
		// GIVEN
		s := NewShutdown()

		// WHEN
		err1 := s.Add("hook1", func(sig os.Signal) {})
		err2 := s.Add("hook2", func(sig os.Signal) {})

		// THEN
		assert.NoError(t, err1)
		assert.NoError(t, err2)
		assert.Equal(t, 2, len(s.getHooks()))
	})

	t.Run("同じキーで追加するとエラーを返す", func(t *testing.T) {
		// GIVEN
		s := NewShutdown()
		err := s.Add("hook1", func(sig os.Signal) {})
		assert.NoError(t, err)

		// WHEN
		err = s.Add("hook1", func(sig os.Signal) {})

		// THEN
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "hook with the same key already exists")
		assert.Equal(t, 1, len(s.getHooks()))
	})
}

func TestShutdown_Listen(t *testing.T) {
	t.Run("シグナルを受信するとフックが実行される", func(t *testing.T) {
		// GIVEN
		s := NewShutdown()
		var called atomic.Bool
		err := s.Add("test", func(sig os.Signal) {
			called.Store(true)
		})
		assert.NoError(t, err)

		// WHEN: 別 goroutine で Listen を起動し、シグナルを送信
		done := make(chan struct{})
		go func() {
			s.Listen()
			close(done)
		}()

		time.Sleep(10 * time.Millisecond)
		syscall.Kill(syscall.Getpid(), syscall.SIGINT)

		// THEN
		select {
		case <-done:
			assert.True(t, called.Load())
		case <-time.After(1 * time.Second):
			t.Fatal("Listen did not return within timeout")
		}
	})

	t.Run("複数のフックがすべて実行される", func(t *testing.T) {
		// GIVEN
		s := NewShutdown()
		var count atomic.Int32
		err := s.Add("hook1", func(sig os.Signal) { count.Add(1) })
		assert.NoError(t, err)
		err = s.Add("hook2", func(sig os.Signal) { count.Add(1) })
		assert.NoError(t, err)

		// WHEN
		done := make(chan struct{})
		go func() {
			s.Listen()
			close(done)
		}()

		time.Sleep(10 * time.Millisecond)
		syscall.Kill(syscall.Getpid(), syscall.SIGINT)

		// THEN
		select {
		case <-done:
			assert.Equal(t, int32(2), count.Load())
		case <-time.After(1 * time.Second):
			t.Fatal("Listen did not return within timeout")
		}
	})

	t.Run("フックに受信したシグナルが渡される", func(t *testing.T) {
		// GIVEN
		s := NewShutdown()
		var received atomic.Value
		err := s.Add("test", func(sig os.Signal) {
			received.Store(sig)
		})
		assert.NoError(t, err)

		// WHEN
		done := make(chan struct{})
		go func() {
			s.Listen()
			close(done)
		}()

		time.Sleep(10 * time.Millisecond)
		syscall.Kill(syscall.Getpid(), syscall.SIGINT)

		// THEN
		select {
		case <-done:
			assert.Equal(t, syscall.SIGINT, received.Load())
		case <-time.After(1 * time.Second):
			t.Fatal("Listen did not return within timeout")
		}
	})
}
