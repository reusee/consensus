package main

import (
	"math/rand"
	"runtime"
)

type any = interface{}

type Routine func(args ...any) Routine

func bind(r Routine, args ...any) Routine {
	return func(_ ...any) Routine {
		return r(args...)
	}
}

var (
	schedIn  = make(chan Routine)
	schedOut = make(chan Routine)
)

func spawn(r Routine) {
	schedIn <- r
}

func init() {
	// scheduler
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			for r := range schedOut {
				r = r()
				if r != nil {
					spawn(r)
				}
			}
		}()
	}
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			var rs []Routine
			for {
				if len(rs) > 0 {
					idx := rand.Intn(len(rs))
					select {
					case r := <-schedIn:
						select {
						case schedOut <- r:
						default:
							rs = append(rs, r)
						}
					case schedOut <- rs[idx]:
						rs = append(
							rs[:idx],
							rs[idx+1:]...,
						)
					}
				} else {
					select {
					case r := <-schedIn:
						select {
						case schedOut <- r:
						default:
							rs = append(rs, r)
						}
					}
				}
			}
		}()
	}
}
