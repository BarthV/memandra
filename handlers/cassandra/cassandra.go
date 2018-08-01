package cassandra

import (
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
	"github.com/netflix/rend/metrics"
	"github.com/netflix/rend/timer"
	"github.com/spf13/viper"
)

type Handler struct {
	session      *gocql.Session
	setbuffer    chan CassandraSet
	buffertimer  *time.Timer
	readonlymode bool
}

type CassandraSet struct {
	Key     []byte
	Data    []byte
	Flags   uint32
	Exptime uint32
}

// Cassandra Batching metrics
var (
	MetricSetBufferSize      = metrics.AddIntGauge("cmd_set_batch_buffer_size", nil)
	MetricCmdSetBatch        = metrics.AddCounter("cmd_set_batch", nil)
	MetricCmdSetBatchErrors  = metrics.AddCounter("cmd_set_batch_errors", nil)
	MetricCmdSetBatchSuccess = metrics.AddCounter("cmd_set_batch_success", nil)
	HistSetBatch             = metrics.AddHistogram("set_batch", false, nil)
	HistSetBufferWait        = metrics.AddHistogram("set_batch_buffer_timewait", false, nil)
)

var singleton *Handler

// SetReadonlyMode switch Cassandra handler to readonly mode for graceful exit
func SetReadonlyMode() {
	singleton.readonlymode = true
}

func bufferSizeCheckLoop() {
	ticker := time.NewTicker(5 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			if len(singleton.setbuffer) >= viper.GetInt("CassandraBatchMinItemSize") {
				go FlushBuffer()
			}
		}
	}
}

// FlushBuffer triggers a batched write operation in Cassandra target
func FlushBuffer() {
	chanLen := len(singleton.setbuffer)
	metrics.SetIntGauge(MetricSetBufferSize, uint64(chanLen))

	if chanLen > 0 {
		metrics.IncCounter(MetricCmdSetBatch)

		// fmt.Println(chanLen)
		if chanLen >= viper.GetInt("CassandraBatchMaxItemSize") {
			chanLen = viper.GetInt("CassandraBatchMaxItemSize")
		}
		b := singleton.session.NewBatch(gocql.UnloggedBatch)
		for i := 1; i <= chanLen; i++ {
			item := (<-singleton.setbuffer)
			b.Query(
				fmt.Sprintf(
					"INSERT INTO %s.%s (keycol,valuecol) VALUES (?, ?) USING TTL ?",
					viper.GetString("CassandraKeyspace"),
					viper.GetString("CassandraBucket"),
				),
				item.Key,
				item.Data,
				item.Exptime,
			)
		}

		// exec CQL batch
		start := timer.Now()
		err := singleton.session.ExecuteBatch(b)
		if err != nil {
			metrics.IncCounter(MetricCmdSetBatchErrors)
			log.Println("[ERROR] Batched Cassandra SET returned an error. ", err)
		} else {
			metrics.IncCounter(MetricCmdSetBatchSuccess)
			metrics.ObserveHist(HistSetBatch, timer.Since(start))
		}
	}

	// TODO: we need to protect this timer reset, and make it thread safe !!
	singleton.buffertimer.Reset(200 * time.Millisecond)
}

// InitCassandraConn initialize Cassandra global connection, call it once before starting ListenAndServe()
func InitCassandraConn() error {
	// Only spawn a unique cassandra session,
	// store this session in a global singleton.
	if singleton == nil {
		clust := gocql.NewCluster(viper.GetString("CassandraHostname"))
		clust.Keyspace = viper.GetString("CassandraKeyspace")
		clust.Consistency = gocql.LocalOne
		clust.Timeout = viper.GetDuration("CassandraTimeoutMs")
		clust.ConnectTimeout = viper.GetDuration("CassandraConnectTimeoutMs")
		sess, err := clust.CreateSession()
		if err != nil {
			return err
		}

		singleton = &Handler{
			session:      sess,
			setbuffer:    make(chan CassandraSet, viper.GetInt("CassandraBatchBufferItemSize")),
			buffertimer:  time.AfterFunc(viper.GetDuration("CassandraBatchBufferMaxAgeMs"), FlushBuffer),
			readonlymode: false,
		}

		go bufferSizeCheckLoop()
	}

	// TODO : prepare Cassandra statements for common queries
	// Currently using session.bind() seems to registers statements if they don't exists in C*

	return nil
}

func New() (handlers.Handler, error) {

	return singleton, nil
}

func (h *Handler) Close() error {

	return nil
}

func computeExpTime(Exptime uint32) uint32 {
	// Maximum allowed relative TTL in memcached protocol
	max_ttl := uint32(60 * 60 * 24 * 30) // number of seconds in 30 days
	if Exptime > max_ttl {
		return Exptime - uint32(time.Now().Unix())
	}
	return Exptime
}

func (h *Handler) Set(cmd common.SetRequest) error {
	if h.readonlymode {
		return common.ErrItemNotStored
	}
	realExptime := computeExpTime(cmd.Exptime)
	start := timer.Now()
	h.setbuffer <- CassandraSet{
		Key:     cmd.Key,
		Data:    cmd.Data,
		Flags:   cmd.Flags,
		Exptime: realExptime,
	}
	metrics.ObserveHist(HistSetBufferWait, timer.Since(start))
	// TODO : maybe add a set timeout that return "not_stored" in case of buffer error ?
	return nil
}

func (h *Handler) Add(cmd common.SetRequest) error {

	return nil
}

func (h *Handler) Replace(cmd common.SetRequest) error {
	if h.readonlymode {
		return common.ErrItemNotStored
	}

	key_qi := func(q *gocql.QueryInfo) ([]interface{}, error) {
		values := make([]interface{}, 1)
		values[0] = cmd.Key
		return values, nil
	}
	var wtime uint
	if err := h.session.Bind(
		/* TODO: better use "UPDATE ... IF EXISTS" pattern because it make use of
		"Lightweight transactions" and it's more consistent. */
		fmt.Sprintf(
			"SELECT writetime(valuecol) FROM %s.%s WHERE keycol=? LIMIT 1",
			viper.GetString("CassandraKeyspace"),
			viper.GetString("CassandraBucket"),
		),
		key_qi,
	).Scan(&wtime); err == nil {
		realExptime := computeExpTime(cmd.Exptime)
		start := timer.Now()
		h.setbuffer <- CassandraSet{
			Key:     cmd.Key,
			Data:    cmd.Data,
			Flags:   cmd.Flags,
			Exptime: realExptime,
		}
		metrics.ObserveHist(HistSetBufferWait, timer.Since(start))
		return nil
	} else {
		if err.Error() == "not found" {
			return common.ErrKeyNotFound
		} else {
			return common.ErrInternal
		}
	}
}

func (h *Handler) Append(cmd common.SetRequest) error {

	return nil
}

func (h *Handler) Prepend(cmd common.SetRequest) error {

	return nil
}

func (h *Handler) Get(cmd common.GetRequest) (<-chan common.GetResponse, <-chan error) {
	dataOut := make(chan common.GetResponse, len(cmd.Keys))
	errorOut := make(chan error)

	for idx, key := range cmd.Keys {
		key_qi := func(q *gocql.QueryInfo) ([]interface{}, error) {
			values := make([]interface{}, 1)
			values[0] = key
			return values, nil
		}

		var val []byte

		if err := h.session.Bind(
			fmt.Sprintf(
				"SELECT keycol,valuecol FROM %s.%s where keycol=?",
				viper.GetString("CassandraKeyspace"),
				viper.GetString("CassandraBucket"),
			),
			key_qi,
		).Scan(&key, &val); err == nil {
			dataOut <- common.GetResponse{
				Miss:   false,
				Quiet:  cmd.Quiet[idx],
				Opaque: cmd.Opaques[idx],
				Flags:  0,
				Key:    []byte(key),
				Data:   val,
			}
		} else {
			dataOut <- common.GetResponse{
				Miss:   true,
				Quiet:  cmd.Quiet[idx],
				Opaque: cmd.Opaques[idx],
				Key:    []byte(key),
				Data:   nil,
			}
		}
	}

	close(dataOut)
	close(errorOut)
	return dataOut, errorOut
}

func (h *Handler) GetE(cmd common.GetRequest) (<-chan common.GetEResponse, <-chan error) {
	dataOut := make(chan common.GetEResponse, len(cmd.Keys))
	errorOut := make(chan error)

	for idx, key := range cmd.Keys {
		key_qi := func(q *gocql.QueryInfo) ([]interface{}, error) {
			values := make([]interface{}, 1)
			values[0] = key
			return values, nil
		}

		var val []byte
		var ttl uint32

		if err := h.session.Bind(
			fmt.Sprintf(
				"SELECT keycol,valuecol,TTL(valuecol) FROM %s.%s where keycol=?",
				viper.GetString("CassandraKeyspace"),
				viper.GetString("CassandraBucket"),
			),
			key_qi,
		).Scan(&key, &val, &ttl); err == nil {
			dataOut <- common.GetEResponse{
				Miss:    false,
				Quiet:   cmd.Quiet[idx],
				Opaque:  cmd.Opaques[idx],
				Flags:   0,
				Key:     []byte(key),
				Data:    val,
				Exptime: ttl,
			}
		} else {
			dataOut <- common.GetEResponse{
				Miss:   true,
				Quiet:  cmd.Quiet[idx],
				Opaque: cmd.Opaques[idx],
				Key:    []byte(key),
				Data:   nil,
			}
		}
	}

	close(dataOut)
	close(errorOut)
	return dataOut, errorOut
}

func (h *Handler) GAT(cmd common.GATRequest) (common.GetResponse, error) {
	return common.GetResponse{
		Miss:   true,
		Opaque: cmd.Opaque,
		Key:    cmd.Key,
	}, nil
}

func (h *Handler) Delete(cmd common.DeleteRequest) error {
	kv_qi := func(q *gocql.QueryInfo) ([]interface{}, error) {
		values := make([]interface{}, 1)
		values[0] = cmd.Key
		return values, nil
	}

	if err := h.session.Bind(
		fmt.Sprintf(
			"DELETE FROM %s.%s WHERE keycol=?",
			viper.GetString("CassandraKeyspace"),
			viper.GetString("CassandraBucket"),
		),
		kv_qi,
	).Exec(); err != nil {
		return err
	}
	return nil
}

func (h *Handler) Touch(cmd common.TouchRequest) error {

	return nil
}
