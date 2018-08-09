package main

import (
	"log"
	"net/http"
	"os"
	"runtime/debug"

	"github.com/BarthV/memandra/handlers/redis"
	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/orcas"
	"github.com/netflix/rend/protocol"
	"github.com/netflix/rend/protocol/binprot"
	"github.com/netflix/rend/protocol/textprot"
	"github.com/netflix/rend/server"
	"github.com/spf13/viper"
)

func init_default_config() {
	log.Println("Initializing configuration")
	viper.SetDefault("ListenPort", 11221)
	viper.SetDefault("InternalMetricsListenAddress", ":11299")
	viper.SetDefault("RedisHostPort", "127.0.0.1:6379")
}

func load_config_from_env() {
	log.Println("Mapping configuration from environment")
	viper.BindEnv("ListenPort", "LISTENPORT")
	viper.BindEnv("InternalMetricsListenAddress", "METRICSLISTENADDR")
	viper.BindEnv("RedisHostPort", "REDISHOSTPORT")
}

func main() {
	if _, set := os.LookupEnv("GOGC"); !set {
		debug.SetGCPercent(100)
	}

	init_default_config()
	load_config_from_env()

	// http debug and metrics endpoint
	go http.ListenAndServe(viper.GetString("InternalMetricsListenAddress"), nil)

	// metrics output prefix
	// metrics.SetPrefix("memandra_")

	var h1 handlers.HandlerConst
	var h2 handlers.HandlerConst

	// L1Only MODE
	h1 = redis.New
	h2 = handlers.NilHandler

	redis.InitRedisConn()

	l := server.TCPListener(viper.GetInt("ListenPort"))
	ps := []protocol.Components{binprot.Components, textprot.Components}

	server.ListenAndServe(l, ps, server.Default, orcas.L1Only, h1, h2)
}
