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
	// GET -> L1 HIT
	t.Run("Get", func(t *testing.T) {
		t.Run("L1Hit", func(t *testing.T) {
			h1 := &testHandler{
				responses: []common.GetResponse{
					{
						Key:  []byte("key"),
						Data: []byte("foo"),
					},
				},
			}
			h2 := &testHandler{}
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

		// GET -> L1 MISS -> L2 HIT ( -> BACKFILL NOT TESTED )
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

		// GET -> L1 MISS -> L2 MISS
		t.Run("L2Miss", func(t *testing.T) {
			h1 := &testHandler{
				errors: []error{common.ErrKeyNotFound},
			}
			h2 := &testHandler{}
			output := &bytes.Buffer{}

			l1l2 := orcas.L1L2Cassandra(h1, h2, textprot.NewTextResponder(bufio.NewWriter(output)))

			err := l1l2.Get(common.GetRequest{
				Keys:    [][]byte{[]byte("key")},
				Opaques: []uint32{0},
				Quiet:   []bool{false},
				NoopEnd: false,
			})
			if err != common.ErrKeyNotFound {
				t.Fatalf("Error should be %s, got %v", common.ErrKeyNotFound, err)
			}

			out := string(output.Bytes())

			t.Logf(out)
			gold := ""

			if out != gold {
				t.Fatalf("Expected response '%v' but got '%v'", gold, out)
			}

			h1.verifyEmpty(t)
			h2.verifyEmpty(t)
		})
	})

	// SET -> L2 OK -> L1 OK
	t.Run("Set", func(t *testing.T) {
		t.Run("L2SetSuccess", func(t *testing.T) {
			t.Run("L1SetSuccess", func(t *testing.T) {
				h1 := &testHandler{
					errors: []error{nil},
				}
				h2 := &testHandler{
					errors: []error{nil},
				}
				output := &bytes.Buffer{}

				l1l2 := orcas.L1L2Cassandra(h1, h2, textprot.NewTextResponder(bufio.NewWriter(output)))

				err := l1l2.Set(common.SetRequest{})
				if err != nil {
					t.Fatalf("Error should be nil, got %v", err)
				}

				out := string(output.Bytes())

				t.Logf(out)

				if out != "STORED\r\n" {
					t.Fatalf("Expected response 'STORED\\r\\n' but got '%v'", out)
				}

				h1.verifyEmpty(t)
				h2.verifyEmpty(t)
			})

			// SET -> L2 OK -> L1 FAIL
			t.Run("L1SetSuccess", func(t *testing.T) {
				h1 := &testHandler{
					errors: []error{common.ErrInternal},
				}
				h2 := &testHandler{
					errors: []error{nil},
				}
				output := &bytes.Buffer{}

				l1l2 := orcas.L1L2Cassandra(h1, h2, textprot.NewTextResponder(bufio.NewWriter(output)))

				err := l1l2.Set(common.SetRequest{})
				if err != nil {
					t.Fatalf("Error should be nil, got %v", err)
				}

				out := string(output.Bytes())

				t.Logf(out)

				if out != "STORED\r\n" {
					t.Fatalf("Expected response 'STORED\\r\\n' but got '%v'", out)
				}

				h1.verifyEmpty(t)
				h2.verifyEmpty(t)
			})

			// SET -> L2 FAIL -> L1 OK
			t.Run("L1SetSuccess", func(t *testing.T) {
				h1 := &testHandler{
					errors: []error{nil},
				}
				h2 := &testHandler{
					errors: []error{common.ErrInternal},
				}
				output := &bytes.Buffer{}

				l1l2 := orcas.L1L2Cassandra(h1, h2, textprot.NewTextResponder(bufio.NewWriter(output)))

				err := l1l2.Set(common.SetRequest{})
				if err != nil {
					t.Fatalf("Error should be nil, got %v", err)
				}

				out := string(output.Bytes())

				t.Logf(out)

				if out != "STORED\r\n" {
					t.Fatalf("Expected response 'STORED\\r\\n' but got '%v'", out)
				}

				h1.verifyEmpty(t)
				h2.verifyEmpty(t)
			})

			// SET -> L2 FAIL -> L1 FAIL
			t.Run("L1SetSuccess", func(t *testing.T) {
				h1 := &testHandler{
					errors: []error{common.ErrItemNotStored},
				}
				h2 := &testHandler{
					errors: []error{common.ErrInternal},
				}
				output := &bytes.Buffer{}

				l1l2 := orcas.L1L2Cassandra(h1, h2, textprot.NewTextResponder(bufio.NewWriter(output)))

				err := l1l2.Set(common.SetRequest{})
				if err != orcas.ErrL1L2SetFailed {
					t.Fatalf("Error should be %s, got %v", orcas.ErrL1L2SetFailed, err)
				}

				out := string(output.Bytes())

				t.Logf(out)

				if out != "" {
					t.Fatalf("Expected response 'STORED\\r\\n' but got '%v'", out)
				}

				h1.verifyEmpty(t)
				h2.verifyEmpty(t)
			})
		})
	})

	// DELETE -> L2 OK -> L1 OK
	t.Run("Delete", func(t *testing.T) {
		t.Run("L2DeleteSuccess", func(t *testing.T) {
			t.Run("L1DeleteSuccess", func(t *testing.T) {
				h1 := &testHandler{
					errors: []error{nil},
				}
				h2 := &testHandler{
					errors: []error{nil},
				}
				output := &bytes.Buffer{}

				l1l2 := orcas.L1L2Cassandra(h1, h2, textprot.NewTextResponder(bufio.NewWriter(output)))

				err := l1l2.Delete(common.DeleteRequest{
					Key:    []byte("key"),
					Opaque: 0,
					Quiet:  false,
				})
				if err != nil {
					t.Fatalf("Error should be nil, got %v", err)
				}

				out := string(output.Bytes())

				t.Logf(out)
				gold := "DELETED\r\n"

				if out != gold {
					t.Fatalf("Expected response '%v' but got '%v'", gold, out)
				}

				h1.verifyEmpty(t)
				h2.verifyEmpty(t)
			})
		})
	})
}