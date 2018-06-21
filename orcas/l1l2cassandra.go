// Copyright 2018 Criteo
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package orcas

import (
	"errors"
	"log"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/orcas"
	"github.com/netflix/rend/protocol"
	"github.com/netflix/rend/timer"
)

var (
	// L1L2Cassandra specific errors
	ErrL1L2SetFailed    = errors.New("ERROR Both L1 & L2 Set failed")
	ErrL1SetAsyncFailed = errors.New("ERROR L1 Async Set failed")

	// L1 Set after failed L2 set attempt metrics
	MetricCmdSetL1NotInL2        = metrics.AddCounter("cmd_set_l1_not_in_l2", nil)
	MetricCmdSetSuccessL1NotInL2 = metrics.AddCounter("cmd_set_l1_success_not_in_l2", nil)
	MetricCmdSetErrorsL1NotInL2  = metrics.AddCounter("cmd_set_l1_errors_not_in_l2", nil)
)

type L1L2CassandraOrca struct {
	l1  handlers.Handler
	l2  handlers.Handler
	res protocol.Responder
}

func L1L2Cassandra(l1, l2 handlers.Handler, res protocol.Responder) orcas.Orca {
	return &L1L2CassandraOrca{
		l1:  l1,
		l2:  l2,
		res: res,
	}
}

func (l *L1L2CassandraOrca) Set(req common.SetRequest) error {
	//log.Println("set", string(req.Key))

	// Set to L2 first
	metrics.IncCounter(orcas.MetricCmdSetL2)
	start := timer.Now()
	errL2 := l.l2.Set(req)

	metrics.ObserveHist(orcas.HistSetL2, timer.Since(start))

	if errL2 != nil {
		metrics.IncCounter(orcas.MetricCmdSetErrorsL2)
	} else {
		// Set to L1
		metrics.IncCounter(orcas.MetricCmdSetL1)
		start := timer.Now()
		errL1 := l.l1.Set(req)

		metrics.ObserveHist(orcas.HistSetL1, timer.Since(start))

		// Log an an error in metrics if data is not stored in L1
		if errL1 != nil {
			metrics.IncCounter(orcas.MetricCmdSetErrorsL1)
		} else {
			metrics.IncCounter(orcas.MetricCmdSetSuccessL1)
		}

		// Successful write to L2 is a completed write !
		metrics.IncCounter(orcas.MetricCmdSetSuccessL2)
		metrics.IncCounter(orcas.MetricCmdSetSuccess)
		return l.res.Set(req.Opaque, req.Quiet)
	}

	// If L2 Set failed, fallback to L1 Set
	// WARN : THIS IS A NON PERSISTED STORE AND IT SHOULD RAISE AN ALERT
	metrics.IncCounter(MetricCmdSetL1NotInL2)
	metrics.IncCounter(orcas.MetricCmdSetL1)
	start = timer.Now()
	errL1 := l.l1.Set(req)

	metrics.ObserveHist(orcas.HistSetL1, timer.Since(start))

	// Return an error if data is not stored in L1 either
	if errL1 != nil {
		metrics.IncCounter(MetricCmdSetErrorsL1NotInL2)
		metrics.IncCounter(orcas.MetricCmdSetErrorsL1)
		metrics.IncCounter(orcas.MetricCmdSetErrors)
		return ErrL1L2SetFailed
	}

	metrics.IncCounter(orcas.MetricCmdSetSuccessL1)
	metrics.IncCounter(orcas.MetricCmdSetSuccess)
	return l.res.Set(req.Opaque, req.Quiet)
}

func (l *L1L2CassandraOrca) Add(req common.SetRequest) error {
	// Add is not yet implemented.
	log.Println("[WARN] Add command not supported by L1L2 NotConsistent orchestrator")
	return common.ErrUnknownCmd
}

func (l *L1L2CassandraOrca) Replace(req common.SetRequest) error {
	//log.Println("replace", string(req.Key))

	// FAST PATH : if key exists in L1 just do a simple set in L2
	// SLOW PATH : L1 is missing, we need to ask L2 if the key exists (it's slower)
	metrics.IncCounter(orcas.MetricCmdReplaceL1)
	start := timer.Now()
	err := l.l1.Replace(req)
	metrics.ObserveHist(orcas.HistReplaceL1, timer.Since(start))

	if err != nil {
		// Key does not exists in L1 ... GO SLOW PATH !
		if err == common.ErrKeyNotFound {
			metrics.IncCounter(orcas.MetricCmdReplaceNotStoredL1)
		} else {
			metrics.IncCounter(orcas.MetricCmdReplaceErrorsL1)
			metrics.IncCounter(orcas.MetricCmdReplaceErrors)
		}

		// Replace in L2 (SLOW PATH)
		metrics.IncCounter(orcas.MetricCmdReplaceL2)
		start := timer.Now()
		err := l.l2.Replace(req)
		metrics.ObserveHist(orcas.HistReplaceL2, timer.Since(start))
		if err != nil {
			if err == common.ErrKeyNotFound {
				// Replacing a key that doesn't exist is a normal error
				metrics.IncCounter(orcas.MetricCmdReplaceNotStoredL2)
				metrics.IncCounter(orcas.MetricCmdReplaceNotStored)
				return common.ErrItemNotStored // memcached return a NOT_STORED msg in this case
			}
			metrics.IncCounter(orcas.MetricCmdReplaceErrorsL2)
			// L1 Not Stored/Error + L2 error ==> Global Error
			metrics.IncCounter(orcas.MetricCmdReplaceErrors)
			return err
		}
		// Slow path successfully completed
		metrics.IncCounter(orcas.MetricCmdReplaceStoredL2)
		metrics.IncCounter(orcas.MetricCmdReplaceStored)
		return l.res.Replace(req.Opaque, req.Quiet)
	} else {
		// L1 has the key, we can assume L2 has it too. We can add a set in L2 buffer
		// So .. This is the FAST PATH !!
		metrics.IncCounter(orcas.MetricCmdReplaceStoredL1)

		// Make a simple set to L2
		metrics.IncCounter(orcas.MetricCmdSetL2)
		start := timer.Now()
		err := l.l2.Set(req)
		metrics.ObserveHist(orcas.HistSetL2, timer.Since(start))

		if err != nil {
			log.Println("[ERROR] Replace success in L1, but simple SET in L2 failed ! (Replace FAST PATH)")
			metrics.IncCounter(MetricCmdSetL1NotInL2)
			metrics.IncCounter(orcas.MetricCmdSetErrorsL2)
			metrics.IncCounter(orcas.MetricCmdReplaceErrorsL2)
			metrics.IncCounter(orcas.MetricCmdReplaceErrors)
			return err
		}
		// Fast path completed successfully
		metrics.IncCounter(orcas.MetricCmdSetSuccessL2)
		metrics.IncCounter(orcas.MetricCmdReplaceStoredL2)
		metrics.IncCounter(orcas.MetricCmdReplaceStored)
		return l.res.Replace(req.Opaque, req.Quiet)
	}
}

func (l *L1L2CassandraOrca) Append(req common.SetRequest) error {
	// Append is not yet implemented.
	log.Println("[WARN] Append command not supported by L1L2 NotConsistent orchestrator")
	return common.ErrUnknownCmd
}

func (l *L1L2CassandraOrca) Prepend(req common.SetRequest) error {
	// Prepend is not yet implemented.
	log.Println("[WARN] Prepend command not supported by L1L2 NotConsistent orchestrator")
	return common.ErrUnknownCmd
}

func (l *L1L2CassandraOrca) Delete(req common.DeleteRequest) error {
	//log.Println("delete", string(req.Key))

	// Try L2 first
	metrics.IncCounter(orcas.MetricCmdDeleteL2)
	start := timer.Now()

	err := l.l2.Delete(req)

	metrics.ObserveHist(orcas.HistDeleteL2, timer.Since(start))

	if err != nil {
		// On a delete miss in L2 don't bother deleting in L1. There might be no
		// key at all, or another request may be deleting the same key. In that
		// case the other will finish up. Returning a key not found will trigger
		// error handling to send back an error response.
		if err == common.ErrKeyNotFound {
			metrics.IncCounter(orcas.MetricCmdDeleteMissesL2)
			metrics.IncCounter(orcas.MetricCmdDeleteMisses)
			return err
		}

		// If we fail to delete in L2, don't delete in L1. This can leave us in
		// an inconsistent state if the request succeeded in L2 but some
		// communication error caused the problem. In the typical deployment of
		// rend, the L1 and L2 caches are both on the same box with
		// communication happening over a unix domain socket. In this case, the
		// likelihood of this error path happening is very small.
		metrics.IncCounter(orcas.MetricCmdDeleteErrorsL2)
		metrics.IncCounter(orcas.MetricCmdDeleteErrors)
		return err
	}
	metrics.IncCounter(orcas.MetricCmdDeleteHitsL2)

	// Now delete in L1. This means we're temporarily inconsistent, but also
	// eliminated the interleaving where the data is deleted from L1, read from
	// L2, set in L1, then deleted in L2. By deleting from L2 first, if L1 goes
	// missing then no other request can undo part of this request.
	metrics.IncCounter(orcas.MetricCmdDeleteL1)
	start = timer.Now()

	err = l.l1.Delete(req)

	metrics.ObserveHist(orcas.HistDeleteL1, timer.Since(start))

	if err != nil {
		// Delete misses in L1 are fine. If we get here, that means the delete
		// in L2 hit. This isn't a miss per se since the overall effect is a
		// delete. Concurrent deletes might interleave to produce this, or the
		// data might have TTL'd out. Both cases are still fine.
		if err == common.ErrKeyNotFound {
			metrics.IncCounter(orcas.MetricCmdDeleteMissesL1)
			metrics.IncCounter(orcas.MetricCmdDeleteHits)
			// disregard the miss, don't return the error
			return l.res.Delete(req.Opaque)
		}
		metrics.IncCounter(orcas.MetricCmdDeleteErrorsL1)
		metrics.IncCounter(orcas.MetricCmdDeleteErrors)
		return err
	}

	metrics.IncCounter(orcas.MetricCmdDeleteHitsL1)
	metrics.IncCounter(orcas.MetricCmdDeleteHits)

	return l.res.Delete(req.Opaque)
}

func (l *L1L2CassandraOrca) Touch(req common.TouchRequest) error {
	// Touch is not yet implemented.
	log.Println("[WARN] Touch command not supported by L1L2 NotConsistent orchestrator")
	return common.ErrUnknownCmd
}

func (l *L1L2CassandraOrca) Get(req common.GetRequest) error {
	metrics.IncCounterBy(orcas.MetricCmdGetKeys, uint64(len(req.Keys)))
	//debugString := "get"
	//for _, k := range req.Keys {
	//	debugString += " "
	//	debugString += string(k)
	//}
	//println(debugString)

	metrics.IncCounter(orcas.MetricCmdGetL1)
	metrics.IncCounterBy(orcas.MetricCmdGetKeysL1, uint64(len(req.Keys)))
	start := timer.Now()

	resChan, errChan := l.l1.Get(req)

	var err error
	//var lastres common.GetResponse
	var l2keys [][]byte
	var l2opaques []uint32
	var l2quiets []bool

	// Read all the responses back from L1.
	// The contract is that the resChan will have GetResponse's for get hits and misses,
	// and the errChan will have any other errors, such as an out of memory error from
	// memcached. If any receive happens from errChan, there will be no more responses
	// from resChan.
	for {
		select {
		case res, ok := <-resChan:
			if !ok {
				resChan = nil
			} else {
				if res.Miss {
					metrics.IncCounter(orcas.MetricCmdGetMissesL1)
					l2keys = append(l2keys, res.Key)
					l2opaques = append(l2opaques, res.Opaque)
					l2quiets = append(l2quiets, res.Quiet)

					/* getres := common.GetResponse{
						Key:    res.Key,
						Flags:  res.Flags,
						Data:   res.Data,
						Miss:   res.Miss,
						Opaque: res.Opaque,
						Quiet:  res.Quiet,
					}

					l.res.Get(getres) */
				} else {
					metrics.IncCounter(orcas.MetricCmdGetHits)
					metrics.IncCounter(orcas.MetricCmdGetHitsL1)

					l.res.Get(res)
				}
			}

		case getErr, ok := <-errChan:
			if !ok {
				errChan = nil
			} else {
				metrics.IncCounter(orcas.MetricCmdGetErrors)
				metrics.IncCounter(orcas.MetricCmdGetErrorsL1)
				err = getErr
			}
		}

		if resChan == nil && errChan == nil {
			break
		}
	}

	// finish up metrics for overall L1 (batch) get operation
	metrics.ObserveHist(orcas.HistGetL1, timer.Since(start))

	// leave early on all hits OR a get error in L1
	if len(l2keys) == 0 {
		if err != nil {
			return err
		}
		return l.res.GetEnd(req.NoopOpaque, req.NoopEnd)
	}

	// Time for the same dance with L2
	req = common.GetRequest{
		Keys:       l2keys,
		NoopEnd:    req.NoopEnd,
		NoopOpaque: req.NoopOpaque,
		Opaques:    l2opaques,
		Quiet:      l2quiets,
	}

	metrics.IncCounter(orcas.MetricCmdGetEL2)
	metrics.IncCounterBy(orcas.MetricCmdGetEKeysL2, uint64(len(l2keys)))
	start = timer.Now()

	resChanE, errChan := l.l2.GetE(req)

	for {
		select {
		case res, ok := <-resChanE:
			if !ok {
				resChanE = nil
			} else {
				if res.Miss {
					metrics.IncCounter(orcas.MetricCmdGetEMissesL2)
					// Missing L2 means a true miss
					metrics.IncCounter(orcas.MetricCmdGetMisses)
				} else {
					metrics.IncCounter(orcas.MetricCmdGetEHitsL2)

					// Fillback L2 result in L1
					setreq := common.SetRequest{
						Key:     res.Key,
						Data:    res.Data,
						Flags:   res.Flags,
						Exptime: res.Exptime,
						Quiet:   res.Quiet,
					}

					metrics.IncCounter(orcas.MetricCmdGetSetL1)
					start2 := timer.Now()

					// Using a Add instead a Set prevent overwriting concurrent Set in L1
					// Add is locking keys in memcached, this creates conccurrency errors!
					// So we're fallbacking to a simple SET as a workaround.
					err = l.l1.Set(setreq)

					metrics.ObserveHist(orcas.HistSetL1, timer.Since(start2))

					if err != nil {
						// L1 Fillback failure is not a "real error", but is still annoying
						// ... This shouldn't happens most of the time though
						metrics.IncCounter(orcas.MetricCmdGetSetErrorsL1)
						err = nil
					} else {
						metrics.IncCounter(orcas.MetricCmdGetSetSucessL1)
					}

					// overall operation is considered as a hit
					metrics.IncCounter(orcas.MetricCmdGetHits)
				}

				getres := common.GetResponse{
					Key:    res.Key,
					Flags:  res.Flags,
					Data:   res.Data,
					Miss:   res.Miss,
					Opaque: res.Opaque,
					Quiet:  res.Quiet,
				}

				l.res.Get(getres)
			}

		case getErr, ok := <-errChan:
			if !ok {
				errChan = nil
			} else {
				metrics.IncCounter(orcas.MetricCmdGetErrors)
				metrics.IncCounter(orcas.MetricCmdGetEErrorsL2)
				err = getErr
			}
		}

		if resChanE == nil && errChan == nil {
			break
		}
	}

	// finish up metrics for overall L2 (batch) get operation
	metrics.ObserveHist(orcas.HistGetL2, timer.Since(start))

	if err == nil {
		return l.res.GetEnd(req.NoopOpaque, req.NoopEnd)
	}

	return err
}

func (l *L1L2CassandraOrca) GetE(req common.GetRequest) error {
	// The L1/L2 batch does not support getE, only L1Only does.
	log.Println("[WARN] Use of unsupported GetE in L1L2 NotConsistent orchestrator")
	return common.ErrUnknownCmd
}

func (l *L1L2CassandraOrca) Gat(req common.GATRequest) error {
	// Get and Touch is not yet implemented.
	log.Println("[WARN] Get & Touch (GAT) command not supported by L1L2 NotConsistent orchestrator")
	return common.ErrUnknownCmd
}

func (l *L1L2CassandraOrca) Noop(req common.NoopRequest) error {
	return l.res.Noop(req.Opaque)
}

func (l *L1L2CassandraOrca) Quit(req common.QuitRequest) error {
	return l.res.Quit(req.Opaque, req.Quiet)
}

func (l *L1L2CassandraOrca) Version(req common.VersionRequest) error {
	return l.res.Version(req.Opaque)
}

func (l *L1L2CassandraOrca) Unknown(req common.Request) error {
	return common.ErrUnknownCmd
}

func (l *L1L2CassandraOrca) Error(req common.Request, reqType common.RequestType, err error) {
	var opaque uint32
	var quiet bool

	if req != nil {
		opaque = req.GetOpaque()
		quiet = req.IsQuiet()
	}

	l.res.Error(opaque, reqType, err, quiet)
}
