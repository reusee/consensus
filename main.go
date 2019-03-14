package main

import (
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var (
	pt = fmt.Printf
)

type (
	Key     int
	Value   int
	RegPair struct {
		Reg   int
		Value Value
	}
	Write struct {
		Key   Key
		Reg   int
		Value *Value
		Regs  []RegPair
		Done  func()
	}
)

func init() {
	var seed int64
	binary.Read(crand.Reader, binary.LittleEndian, &seed)
	rand.Seed(seed)
}

func main() {

	var comb func(selections []int, n int) [][]int
	comb = func(selections []int, n int) (ret [][]int) {
		if n == 0 || len(selections) < n {
			return
		}
		if n == 1 {
			for _, sel := range selections {
				ret = append(ret, []int{sel})
			}
			return
		}
		for i, sel := range selections {
			for _, c := range comb(selections[i+1:], n-1) {
				if len(c) == 0 {
					continue
				}
				newC := make([]int, len(c)+1)
				newC[0] = sel
				copy(newC[1:], c)
				ret = append(ret, newC)
			}
		}
		return
	}

	numRegisterServers := 5
	var servers []int
	for i := 0; i < numRegisterServers; i++ {
		servers = append(servers, i)
	}
	conf := comb(servers, (len(servers)+1)/2)
	configuration := func(reg int) [][]int {
		return conf
	}

	// register servers
	var regWriteChans []chan *Write
	write := func(server int, key Key, register int, value *Value) []RegPair {
		var l sync.Mutex
		l.Lock()
		w := &Write{
			Key:   key,
			Reg:   register,
			Value: value,
			Done: func() {
				l.Unlock()
			},
		}
		regWriteChans[server] <- w
		l.Lock()
		return w.Regs
	}

	for i := 0; i < numRegisterServers; i++ {
		ch := make(chan *Write)
		regWriteChans = append(regWriteChans, ch)
		go func() {
			registers := make(map[Key][]RegPair)
			//TODO unload old entries

			sigSync := make(chan struct{})
			go func() {
			loop:
				for {
					select {
					case w := <-ch:
						pairs, ok := registers[w.Key]
						for _, pair := range pairs {
							if pair.Reg == w.Reg {
								// written
								w.Regs = pairs
								w.Done()
								continue loop
							}
						}
						// write
						if w.Value != nil {
							pairs = append(pairs, RegPair{
								Reg:   w.Reg,
								Value: *w.Value,
							})
							registers[w.Key] = pairs
							if !ok {
								select {
								case sigSync <- struct{}{}:
								default:
								}
							}
						}
						w.Regs = pairs
						w.Done()
					}
				}
			}()

			// minion client
			var key Key
		wait:
			for range sigSync {
			loop_key:
				for {

					var input *Value
					states := make(map[int][]RegPair)
					reg := 0
					decidedServers := make(map[int]bool)
					allAny := true
				loop:
					for {
						quorums := configuration(reg)
					loop_quorum:
						for _, quorum := range quorums {
							var valueSet []Value
							var firstNull *int
							for i, server := range quorum {
								serverState := states[server]
								isNull := true
								for _, pair := range serverState {
									if pair.Reg == reg {
										i := 0
										for ; i < len(valueSet); i++ {
											if valueSet[i] == pair.Value {
												break
											}
										}
										if i == len(valueSet) {
											valueSet = append(valueSet, pair.Value)
										}
										isNull = false
										break
									}
								}
								if isNull && firstNull == nil {
									i := i
									firstNull = &i
								}
							}

							if len(valueSet) == 0 {
								// any
								allAny = false
								server := quorum[rand.Intn(len(quorum))]
								ret := write(server, key, reg, input)
								states[server] = ret
								continue loop

							} else if len(valueSet) == 1 && firstNull == nil {
								// decided
								for _, server := range quorum {
									decidedServers[server] = true
								}
								if len(decidedServers) == numRegisterServers {
									if key%100000 == 0 {
										pt("%d replicated\n", key)
									}
									key++
									continue loop_key
								}
								continue loop_quorum

							} else if len(valueSet) > 1 {
								// none
								continue loop_quorum

							} else if len(valueSet) == 1 && firstNull != nil {
								// maybe
								input = &valueSet[0]
								ret := write(quorum[*firstNull], key, reg, input)
								states[quorum[*firstNull]] = ret
								continue loop
							}

							panic("impossible")

						}

						reg++
					}
					if allAny {
						continue wait
					}

				}
			}

		}()
	}

	// handler servers
	type Request struct {
		Value Value
		Key   Key
		Done  func()
	}
	reqChan := make(chan *Request)
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {

			var nextKey Key
			kv := make(map[Key]Value)

		loop_reqs:
			for {
				select {

				case req := <-reqChan:
				do:
					input := &req.Value
					states := make(map[int][]RegPair)
					reg := 0

				loop:
					for {
						quorums := configuration(reg)
						for _, quorum := range quorums {
							var valueSet []Value
							var firstNull *int
							for i, server := range quorum {
								serverState := states[server]
								isNull := true
								for _, pair := range serverState {
									if pair.Reg == reg {
										i := 0
										for ; i < len(valueSet); i++ {
											if valueSet[i] == pair.Value {
												break
											}
										}
										if i == len(valueSet) {
											valueSet = append(valueSet, pair.Value)
										}
										isNull = false
										break
									}
								}
								if isNull && firstNull == nil {
									i := i
									firstNull = &i
								}
							}

							if len(valueSet) == 0 {
								// any
								server := quorum[rand.Intn(len(quorum))]
								ret := write(server, nextKey, reg, input)
								states[server] = ret
								continue loop

							} else if len(valueSet) == 1 && firstNull == nil {
								// decided
								kv[nextKey] = valueSet[0]
								if valueSet[0] == req.Value {
									// ok
									req.Key = nextKey
									nextKey++
									req.Done()
									continue loop_reqs
								} else {
									nextKey++
									goto do
								}

							} else if len(valueSet) > 1 {
								// none
								continue // next quorum

							} else if len(valueSet) == 1 && firstNull != nil {
								// maybe
								if valueSet[0] != req.Value {
									// fail fast
									nextKey++
									goto do
								}
								input = &valueSet[0]
								ret := write(quorum[*firstNull], nextKey, reg, input)
								states[quorum[*firstNull]] = ret
								continue loop
							}

							panic("impossible")

						}

						reg++
					}

				}
			}

		}()
	}

	// client requests
	var n int64
	t0 := time.Now()
	for i := 0; i <= 1000000; i++ {
		spawn(func(_ ...any) Routine {
			req := new(Request)
			req.Value = Value(rand.Int63())
			req.Done = func() {
				if c := atomic.AddInt64(&n, 1); c%10000 == 0 {
					pt("%d %v\n", c, time.Since(t0))
				}
			}
			reqChan <- req
			return nil
		})
	}

	select {}

}
