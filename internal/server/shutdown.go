package server

import (
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// Shutdown は Ctl + C などでの終了シグナルを受け取った際に実行されるフックを管理する
type Shutdown struct {
	hooks map[string]func(os.Signal) // シグナル受信時に実行されるフックのマップ
	mutex *sync.Mutex                // hooks のマップへの同時アクセスを防ぐための mutex
}

func NewShutdown() *Shutdown {
	return &Shutdown{
		hooks: map[string]func(os.Signal){},
		mutex: &sync.Mutex{},
	}
}

// Add はフックを追加する
func (s *Shutdown) Add(key string, fn func(os.Signal)) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, exists := s.hooks[key]; exists {
		return errors.New("hook with the same key already exists")
	}
	s.hooks[key] = fn
	return nil
}

// Listen は指定された OS シグナルを待ち受け、受信したら登録されたフックをすべて実行する
func (s *Shutdown) Listen() {
	trap := make(chan os.Signal, 1)
	signal.Notify(trap, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGINT)
	sig := <-trap

	var wg sync.WaitGroup
	for _, fn := range s.getHooks() {
		wg.Add(1)
		go func(sig os.Signal, fn func(os.Signal)) {
			defer wg.Done()
			fn(sig)
		}(sig, fn)
	}
	wg.Wait()
}

// getHooks は登録されているフックをすべて取得する
func (s *Shutdown) getHooks() map[string]func(os.Signal) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	fns := map[string]func(os.Signal){}
	for key, cb := range s.hooks {
		fns[key] = cb
	}
	return fns
}
