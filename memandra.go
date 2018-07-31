package main

import (
	"log"
	"net/http"
	"os"
	"runtime/debug"
	"time"

	"github.com/BarthV/memandra/handlers/cassandra"
	"github.com/BarthV/memandra/orcas"
	"github.com/netflix/rend/handlers"
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
	viper.SetDefault("CassandraHostname", "127.0.0.1")
	viper.SetDefault("CassandraKeyspace", "kvstore")
	viper.SetDefault("CassandraBucket", "bucket")
	viper.SetDefault("CassandraBatchBufferItemSize", 80000)
	viper.SetDefault("CassandraBatchBufferMaxAgeMs", 200*time.Millisecond)
	viper.SetDefault("CassandraBatchMinItemSize", 1000)
	viper.SetDefault("CassandraBatchMaxItemSize", 5000)
	viper.SetDefault("CassandraTimeoutMs", 1000*time.Millisecond)
	viper.SetDefault("CassandraConnectTimeoutMs", 1000*time.Millisecond)
}

func load_config_from_env() {
	log.Println("Mapping configuration from environment")
	viper.BindEnv("ListenPort", "LISTENPORT")
	viper.BindEnv("InternalMetricsListenAddress", "METRICSLISTENADDR")
	viper.BindEnv("CassandraHostname", "CASSANDRAHOST")
	viper.BindEnv("CassandraKeyspace", "CASSANDRAKEYSPACE")
	viper.BindEnv("CassandraBucket", "CASSANDRABUCKET")
	viper.BindEnv("CassandraBatchBufferItemSize", "BUFFERITEMSIZE")
	viper.BindEnv("CassandraBatchBufferMaxAgeMs", "BUFFERMAXAGE")
	viper.BindEnv("CassandraBatchMinItemSize", "BATCHMINSIZE")
	viper.BindEnv("CassandraBatchMaxItemSize", "BATCHMAXSIZE")
	viper.BindEnv("CassandraTimeoutMs", "CASSANDRATIMEOUT")
	viper.BindEnv("CassandraConnectTimeoutMs", "CASSANDRACONNTIMEOUT")
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
	h1 = cassandra.New
	h2 = handlers.NilHandler

	l := server.TCPListener(viper.GetInt("ListenPort"))
	ps := []protocol.Components{binprot.Components, textprot.Components}

	server.ListenAndServe(l, ps, server.Default, orcas.L1OnlyCassandra, h1, h2)
}
