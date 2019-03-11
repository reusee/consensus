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
	numServers := 10
	configuration := func(reg int) [][]int {
		return [][]int{
			{9, 8, 7, 6, 5, 4, 3},
			{0, 1, 2, 3, 4, 5, 6},
		}
	}

	type Write struct {
		Arg  [2]int
		Ret  [][2]int
		Done func()
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
							if pair[0] == w.Arg[0] { // written
								w.Ret = registers
								w.Done()
								continue loop
							}
						}
						// write
						registers = append(registers, w.Arg)
						w.Ret = registers
						w.Done()

					case <-sigClose:
						pt("%v\n", registers)
						return
					}
				}
			}()
		}

		write := func(server int, register int, value int) [][2]int {
			var l sync.Mutex
			l.Lock()
			w := &Write{
				Arg: [2]int{register, value},
				Done: func() {
					l.Unlock()
				},
			}
			writeChans[server] <- w
			l.Lock()
			return w.Ret
		}

		// clients
		numClients := 128
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

				if rand.Intn(5) == 0 {
					// no input
					return
				}
				input := int(rand.Int63())

				states := make(map[int][][2]int)

				reg := 0
			loop:
				for {
					quorums := configuration(reg)
					numDecided := 0
					for _, quorum := range quorums {

						values := make([]*int, len(quorum))
						for i, server := range quorum {
							serverState := states[server]
							for _, pair := range serverState {
								if pair[0] == reg {
									values[i] = &pair[1]
									break
								}
							}
						}

						isAny := func() bool {
							for _, v := range values {
								if v != nil {
									return false
								}
							}
							return true
						}()
						if isAny {
							//pt("any\n")
							server := quorum[rand.Intn(len(quorum))]
							ret := write(server, reg, input)
							states[server] = ret
							continue loop
						}

						isDecided := func() bool {
							var compare int
							for i, v := range values {
								if v == nil {
									return false
								}
								if i == 0 {
									compare = *v
								} else if compare != *v {
									return false
								}
							}
							return true
						}()
						if isDecided {
							decides[i] = *values[0]
							//pt("decided %d\n", *values[0])
							numDecided++
							if numDecided == len(quorums) {
								return
							} else {
								// replicate
								continue
							}
						}

						isNone := func() bool {
							var compare *int
							for _, v := range values {
								if v == nil {
									continue
								}
								if compare == nil {
									compare = v
								} else if *compare != *v {
									return true
								}
							}
							return false
						}()
						if isNone {
							//pt("none\n")
							continue
						}

						var maybe *int
						var nextServer int
						isMaybe := func() bool {
							for i, v := range values {
								if v == nil {
									nextServer = i
									continue
								}
								if maybe == nil {
									maybe = v
								} else if *maybe != *v {
									return false
								}
							}
							return true
						}()
						if isMaybe {
							input = *maybe
							//pt("maybe %d\n", input)
							ret := write(quorum[nextServer], reg, input)
							states[quorum[nextServer]] = ret
							continue loop
						}

						panic("bad path")

					}

					// all none
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

		time.Sleep(time.Millisecond * 200)
	}
}
