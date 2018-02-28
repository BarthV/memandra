package main

import (
	"net/http"
	"os"
	"runtime/debug"
	"strconv"

	"github.com/BarthV/memandra/handlers/cassandra"
	"github.com/BarthV/memandra/orcas"
	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/handlers/memcached"
	"github.com/netflix/rend/protocol"
	"github.com/netflix/rend/protocol/binprot"
	"github.com/netflix/rend/protocol/textprot"
	"github.com/netflix/rend/server"
)

func main() {
	if _, set := os.LookupEnv("GOGC"); !set {
		debug.SetGCPercent(100)
	}

	// http debug and metrics endpoint
	go http.ListenAndServe("localhost:11299", nil)

	// metrics output prefix
	// metrics.SetPrefix("memandra_")

	var h1 handlers.HandlerConst
	var h2 handlers.HandlerConst

	// L1Only MODE
	// h1 = cassandra.New
	// h2 = handlers.NilHandler
	//
	// L1L2 MODE
	h1 = memcached.Regular("/var/run/memcached/memcached.sock")
	h2 = cassandra.New

	// TODO : write something better ;)
	port, _ := strconv.Atoi(os.Getenv("PORT0"))
	l := server.TCPListener(port)
	ps := []protocol.Components{binprot.Components, textprot.Components}

	// server.ListenAndServe(l, ps, server.Default, orcas.L1Only, h1, h2)
	// server.ListenAndServe(l, ps, server.Default, orcas.L1L2, h1, h2)
	server.ListenAndServe(l, ps, server.Default, orcas.L1L2Cassandra, h1, h2)
}
