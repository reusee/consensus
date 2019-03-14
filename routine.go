package main

import "runtime"

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
					select {
					case r := <-schedIn:
						select {
						case schedOut <- r:
						default:
							rs = append(rs, r)
						}
					case schedOut <- rs[0]:
						rs = rs[1:]
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
