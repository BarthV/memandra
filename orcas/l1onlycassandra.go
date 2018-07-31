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
	"log"

	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/orcas"
	"github.com/netflix/rend/protocol"
	"github.com/netflix/rend/timer"
)

type L1OnlyCassandraOrca struct {
	l1  handlers.Handler
	res protocol.Responder
}

func L1OnlyCassandra(l1, l2 handlers.Handler, res protocol.Responder) orcas.Orca {
	return &L1OnlyCassandraOrca{
		l1:  l1,
		res: res,
	}
}

func (l *L1OnlyCassandraOrca) Set(req common.SetRequest) error {
	//log.Println("set", string(req.Key))

	metrics.IncCounter(orcas.MetricCmdSetL1)
	start := timer.Now()

	err := l.l1.Set(req)

	metrics.ObserveHist(orcas.HistSetL1, timer.Since(start))

	if err == nil {
		metrics.IncCounter(orcas.MetricCmdSetSuccessL1)
		metrics.IncCounter(orcas.MetricCmdSetSuccess)

		err = l.res.Set(req.Opaque, req.Quiet)

	} else {
		metrics.IncCounter(orcas.MetricCmdSetErrorsL1)
		metrics.IncCounter(orcas.MetricCmdSetErrors)
	}

	return err
}

func (l *L1OnlyCassandraOrca) Add(req common.SetRequest) error {
	// Add is not yet implemented.
	log.Println("[WARN] Add command not supported by L1Only Cassandra orchestrator")
	return common.ErrUnknownCmd
}

func (l *L1OnlyCassandraOrca) Replace(req common.SetRequest) error {
	//log.Println("replace", string(req.Key))

	// Replace in L1 (SLOW PATH) :
	// We need to ask L1 if the key exists before setting the key or not (it's slower)
	metrics.IncCounter(orcas.MetricCmdReplaceL2)
	start := timer.Now()
	err := l.l1.Replace(req)
	metrics.ObserveHist(orcas.HistReplaceL1, timer.Since(start))
	if err != nil {
		if err == common.ErrKeyNotFound {
			// Replacing a key that doesn't exist is a normal error
			metrics.IncCounter(orcas.MetricCmdReplaceNotStoredL1)
			metrics.IncCounter(orcas.MetricCmdReplaceNotStored)
			return common.ErrItemNotStored // memcached return a NOT_STORED msg in this case
		}
		metrics.IncCounter(orcas.MetricCmdReplaceErrorsL1)
		// L1 error ==> Global Error
		metrics.IncCounter(orcas.MetricCmdReplaceErrors)
		return err
	}
	// Slow path successfully completed
	metrics.IncCounter(orcas.MetricCmdReplaceStoredL1)
	metrics.IncCounter(orcas.MetricCmdReplaceStored)
	return l.res.Replace(req.Opaque, req.Quiet)
}

func (l *L1OnlyCassandraOrca) Append(req common.SetRequest) error {
	// Append is not yet implemented.
	log.Println("[WARN] Append command not supported by L1Only Cassandra orchestrator")
	return common.ErrUnknownCmd
}

func (l *L1OnlyCassandraOrca) Prepend(req common.SetRequest) error {
	// Prepend is not yet implemented.
	log.Println("[WARN] Prepend command not supported by L1Only Cassandra orchestrator")
	return common.ErrUnknownCmd
}

func (l *L1OnlyCassandraOrca) Delete(req common.DeleteRequest) error {
	//log.Println("delete", string(req.Key))

	metrics.IncCounter(orcas.MetricCmdDeleteL1)
	start := timer.Now()

	err := l.l1.Delete(req)

	metrics.ObserveHist(orcas.HistDeleteL1, timer.Since(start))

	if err == nil {
		metrics.IncCounter(orcas.MetricCmdDeleteHits)
		metrics.IncCounter(orcas.MetricCmdDeleteHitsL1)

		l.res.Delete(req.Opaque)

	} else if err == common.ErrKeyNotFound {
		metrics.IncCounter(orcas.MetricCmdDeleteMissesL1)
		metrics.IncCounter(orcas.MetricCmdDeleteMisses)
	} else {
		metrics.IncCounter(orcas.MetricCmdDeleteErrorsL1)
		metrics.IncCounter(orcas.MetricCmdDeleteErrors)
	}

	return err
}

func (l *L1OnlyCassandraOrca) Touch(req common.TouchRequest) error {
	// Touch is not yet implemented.
	log.Println("[WARN] Touch command not supported by L1Only Cassandra orchestrator")
	return common.ErrUnknownCmd
}

func (l *L1OnlyCassandraOrca) Get(req common.GetRequest) error {
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

	// Read all the responses back from l.l1.
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
					metrics.IncCounter(orcas.MetricCmdGetMisses)
				} else {
					metrics.IncCounter(orcas.MetricCmdGetHits)
					metrics.IncCounter(orcas.MetricCmdGetHitsL1)
				}
				l.res.Get(res)
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

	metrics.ObserveHist(orcas.HistGetL1, timer.Since(start))

	if err == nil {
		l.res.GetEnd(req.NoopOpaque, req.NoopEnd)
	}

	return err
}

func (l *L1OnlyCassandraOrca) GetE(req common.GetRequest) error {
	// The L1/L2 batch does not support getE, only L1Only does.
	log.Println("[WARN] Use of unsupported GetE in L1Only Cassandra orchestrator")
	return common.ErrUnknownCmd
}

func (l *L1OnlyCassandraOrca) Gat(req common.GATRequest) error {
	// Get and Touch is not yet implemented.
	log.Println("[WARN] Get & Touch (GAT) command not supported by L1Only Cassandra orchestrator")
	return common.ErrUnknownCmd
}

func (l *L1OnlyCassandraOrca) Noop(req common.NoopRequest) error {
	return l.res.Noop(req.Opaque)
}

func (l *L1OnlyCassandraOrca) Quit(req common.QuitRequest) error {
	return l.res.Quit(req.Opaque, req.Quiet)
}

func (l *L1OnlyCassandraOrca) Version(req common.VersionRequest) error {
	return l.res.Version(req.Opaque)
}

func (l *L1OnlyCassandraOrca) Unknown(req common.Request) error {
	return common.ErrUnknownCmd
}

func (l *L1OnlyCassandraOrca) Error(req common.Request, reqType common.RequestType, err error) {
	var opaque uint32
	var quiet bool

	if req != nil {
		opaque = req.GetOpaque()
		quiet = req.IsQuiet()
	}

	l.res.Error(opaque, reqType, err, quiet)
}
