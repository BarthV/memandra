package main

import (
	"os"
	"strconv"

	"github.com/BarthV/memandra/handlers/cassandra"
	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/orcas"
	"github.com/netflix/rend/protocol"
	"github.com/netflix/rend/protocol/binprot"
	"github.com/netflix/rend/protocol/textprot"
	"github.com/netflix/rend/server"
)

func main() {
	var h1 handlers.HandlerConst
	var h2 handlers.HandlerConst

	h1 = cassandra.New
	//h1 = inmem.New
	h2 = handlers.NilHandler

	// TODO : write something better ;)
	port, _ := strconv.Atoi(os.Getenv("PORT0"))
	l := server.TCPListener(port)
	ps := []protocol.Components{binprot.Components, textprot.Components}

	server.ListenAndServe(l, ps, server.Default, orcas.L1Only, h1, h2)
}
