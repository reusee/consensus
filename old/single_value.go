package main

import (
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func init() {
	var seed int64
	binary.Read(crand.Reader, binary.LittleEndian, &seed)
	rand.Seed(seed)
}

var (
	pt = fmt.Printf
)

func main() {
	numServers := 7
	configuration := func(reg int) [][]int {
		return [][]int{
			{0, 1, 2, 3},
			{1, 2, 3, 4},
			{2, 3, 4, 5},
			{3, 4, 5, 6},
		}
	}

	type Write struct {
		Reg   int
		Value *int
		Ret   [][2]int
		Done  func()
	}

	for {
		t0 := time.Now()
		sigClose := make(chan struct{})

		// servers
		var writeChans []chan *Write
		for i := 0; i < numServers; i++ {
			ch := make(chan *Write)
			writeChans = append(writeChans, ch)
			go func() {
				var registers [][2]int
			loop:
				for {
					select {
					case w := <-ch:
						for _, pair := range registers {
							if pair[0] == w.Reg { // written
								w.Ret = registers
								w.Done()
								continue loop
							}
						}
						// write
						if w.Value != nil {
							registers = append(registers, [2]int{w.Reg, *w.Value})
						}
						w.Ret = registers
						w.Done()

					case <-sigClose:
						//pt("%v\n", registers)
						return
					}
				}
			}()
		}

		write := func(server int, register int, value *int) [][2]int {
			var l sync.Mutex
			l.Lock()
			w := &Write{
				Reg:   register,
				Value: value,
				Done: func() {
					l.Unlock()
				},
			}
			writeChans[server] <- w
			l.Lock()
			return w.Ret
		}

		// clients
		numClients := 512
		wg := new(sync.WaitGroup)
		wg.Add(numClients)
		decides := make([]int, numClients)
		for i := 0; i < numClients; i++ {
			i := i

			go func() {
				defer wg.Done()

				pt := func(format string, args ...interface{}) {
					fmt.Printf("%d |> ", i)
					fmt.Printf(format, args...)
				}
				_ = pt

				n := int(rand.Int63())
				input := &n

				states := make(map[int][][2]int)

				reg := 0
			loop:
				for {
					quorums := configuration(reg)
					for _, quorum := range quorums {
						var valueSet []int
						var firstNull *int
						for i, server := range quorum {
							serverState := states[server]
							isNull := true
							for _, pair := range serverState {
								if pair[0] == reg {
									i := 0
									for ; i < len(valueSet); i++ {
										if valueSet[i] == pair[1] {
											break
										}
									}
									if i == len(valueSet) {
										valueSet = append(valueSet, pair[1])
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

						if len(valueSet) == 0 { // any
							server := quorum[rand.Intn(len(quorum))]
							ret := write(server, reg, input)
							states[server] = ret
							continue loop

						} else if len(valueSet) == 1 && firstNull == nil { // decided
							decides[i] = valueSet[0]
							return

						} else if len(valueSet) > 1 { // none
							continue

						} else if len(valueSet) == 1 && firstNull != nil { // maybe
							input = &valueSet[0]
							ret := write(quorum[*firstNull], reg, input)
							states[quorum[*firstNull]] = ret
							continue loop
						}

						panic("impossible")

					}

					// done with current register set
					reg++

				}

			}()
		}
		wg.Wait()

		close(sigClose)

		var compare int
		for i := 0; i < numClients; i++ {
			if decides[i] == 0 {
				continue
			}
			if compare == 0 {
				compare = decides[i]
			} else if decides[i] != compare {
				pt("%d %d\n", decides[i], decides[0])
				panic("bad")
			}
		}
		pt("decide %d in %v\n", compare, time.Since(t0))

		//time.Sleep(time.Millisecond * 200)
	}
}
