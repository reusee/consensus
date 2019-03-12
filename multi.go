package main

import (
	"fmt"
	"math/rand"
	"sync"
)

var (
	pt = fmt.Printf
)

func main() {

	numRegisterServers := 5
	configuration := func(reg int) [][]int {
		return [][]int{
			//{0, 1, 2, 3},
			//{1, 2, 3, 4},
			//{2, 3, 4, 5},
			//{3, 4, 5, 6},

			{0, 1, 2},
			{1, 2, 3},
			{2, 3, 4},
		}
	}

	// register servers
	type Key int
	type Value int
	type RegPair struct {
		Reg   int
		Value Value
	}
	type Write struct {
		Key   Key
		Reg   int
		Value *Value
		Regs  []RegPair
		Done  func()
	}
	var regWriteChans []chan *Write
	for i := 0; i < numRegisterServers; i++ {
		ch := make(chan *Write)
		regWriteChans = append(regWriteChans, ch)
		go func() {
			registers := make(map[Key][]RegPair)
			//TODO unload old entries

		loop:
			for {
				select {
				case w := <-ch:
					pairs := registers[w.Key]
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
					}
					w.Regs = pairs
					w.Done()
				}
			}

		}()
	}

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

	// handler servers
	type Request struct {
		Value Value
		Key   Key
		Done  func()
	}
	reqChan := make(chan *Request)
	for i := 0; i < 64; i++ {
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
	var l sync.Mutex
	kv := make(map[Key]Value)
	for {
		req := new(Request)
		req.Value = Value(rand.Int63())
		req.Done = func() {
			l.Lock()
			if _, ok := kv[req.Key]; ok {
				panic("conflict")
			}
			kv[req.Key] = req.Value
			l.Unlock()
			pt("%v %v\n", req.Key, req.Value)
		}
		reqChan <- req
	}

}
