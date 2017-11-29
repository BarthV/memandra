package main

import (
	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/handlers/inmem"
	"github.com/netflix/rend/orcas"
	"github.com/netflix/rend/protocol"
	"github.com/netflix/rend/protocol/binprot"
	"github.com/netflix/rend/protocol/textprot"
	"github.com/netflix/rend/server"
)

func main() {

	var h2 handlers.HandlerConst
	var h1 handlers.HandlerConst

	h1 = inmem.New
	h2 = handlers.NilHandler

	l := server.TCPListener(11211)
	ps := []protocol.Components{binprot.Components, textprot.Components}

	server.ListenAndServe(l, ps, server.Default, orcas.L1Only, h1, h2)
}
