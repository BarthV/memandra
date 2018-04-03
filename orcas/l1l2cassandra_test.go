// Copyright 2017 Netflix, Inc.
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

package orcas_test

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/BarthV/memandra/orcas"
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/protocol/textprot"
)

func TestL1L2Orca(t *testing.T) {

	t.Run("Get", func(t *testing.T) {
		t.Run("L1Miss", func(t *testing.T) {
			t.Run("L2Hit", func(t *testing.T) {
				h1 := &testHandler{
					errors: []error{common.ErrKeyNotFound},
					responses: []common.GetResponse{
						{
							Miss: true,
						},
					},
				}
				h2 := &testHandler{
					eresponses: []common.GetEResponse{
						{
							Key:  []byte("key"),
							Data: []byte("foo"),
						},
					},
				}
				output := &bytes.Buffer{}

				l1l2 := orcas.L1L2Cassandra(h1, h2, textprot.NewTextResponder(bufio.NewWriter(output)))

				err := l1l2.Get(common.GetRequest{
					Keys:    [][]byte{[]byte("key")},
					Opaques: []uint32{0},
					Quiet:   []bool{false},
					NoopEnd: false,
				})
				if err != nil {
					t.Fatalf("Error should be nil, got %v", err)
				}

				out := string(output.Bytes())

				t.Logf(out)
				gold := "VALUE key 0 3\r\nfoo\r\nEND\r\n"

				if out != gold {
					t.Fatalf("Expected response '%v' but got '%v'", gold, out)
				}

				h1.verifyEmpty(t)
				h2.verifyEmpty(t)
			})
		})
	})
}
