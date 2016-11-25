package app

import (
	"sync"
)

type WaitGroupWrapper struct {
	sync.WaitGroup
}

//simple function wrapper
func (w *WaitGroupWrapper) Wrap(sf func()) {
	w.Add(1)
	go func() {
		sf()
		w.Done()
	}()
}
