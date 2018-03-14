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
	session     *gocql.Session
	setbuffer   chan CassandraSet
	buffertimer *time.Timer
	// flushLock   *sync.Mutex
	// isFlushing  bool
}

type CassandraSet struct {
	Key     []byte
	Data    []byte
	Flags   uint32
	Exptime uint32
}

var (
	// L2Cassandra Batching metrics
	MetricSetBufferSize        = metrics.AddIntGauge("cmd_set_l2_batch_buffer_size", nil)
	MetricCmdSetL2Batch        = metrics.AddCounter("cmd_set_l2_batch", nil)
	MetricCmdSetL2BatchErrors  = metrics.AddCounter("cmd_set_l2_batch_errors", nil)
	MetricCmdSetL2BatchSuccess = metrics.AddCounter("cmd_set_l2_batch_success", nil)
	HistSetL2Batch             = metrics.AddHistogram("set_l2_batch", false, nil)
	HistSetL2BufferWait        = metrics.AddHistogram("set_l2_buffer_timewait", false, nil)
)

var singleton *Handler

func unsetFlushingState() {
	// singleton.isFlushing = false
}

func bufferSizeCheckLoop() {
	ticker := time.NewTicker(5 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			if len(singleton.setbuffer) >= viper.GetInt("CassandraBatchMinItemSize") {
				go flushBuffer()
			}
		}
	}
}

func flushBuffer() {
	/* if singleton.isFlushing {
		return
	}

	singleton.flushLock.Lock()
	singleton.isFlushing = true
	defer unsetFlushingState()
	defer singleton.flushLock.Unlock() */
	chanLen := len(singleton.setbuffer)
	metrics.SetIntGauge(MetricSetBufferSize, uint64(chanLen))

	if chanLen > 0 {
		metrics.IncCounter(MetricCmdSetL2Batch)

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
			metrics.IncCounter(MetricCmdSetL2BatchErrors)
			log.Println("[ERROR] Batched Cassandra SET returned an error. ", err)
		} else {
			metrics.IncCounter(MetricCmdSetL2BatchSuccess)
			metrics.ObserveHist(HistSetL2Batch, timer.Since(start))
		}
	}

	// TODO: we need to protect this timer reset, and make it thread safe !!
	singleton.buffertimer.Reset(200 * time.Millisecond)
}

func New() (handlers.Handler, error) {
	// Only spawn a unique cassandra session per instance,
	// store this session in a global singleton.
	if singleton == nil {
		clust := gocql.NewCluster(viper.GetString("CassandraHostname"))
		clust.Keyspace = viper.GetString("CassandraKeyspace")
		clust.Consistency = gocql.LocalOne
		clust.Timeout = viper.GetDuration("CassandraTimeoutMs")
		clust.ConnectTimeout = viper.GetDuration("CassandraConnectTimeoutMs")
		sess, err := clust.CreateSession()
		if err != nil {
			return nil, err
		}

		singleton = &Handler{
			session:     sess,
			setbuffer:   make(chan CassandraSet, viper.GetInt("CassandraBatchBufferItemSize")),
			buffertimer: time.AfterFunc(viper.GetDuration("CassandraBatchBufferMaxAgeMs"), flushBuffer),
			// flushLock:   &sync.Mutex{},
			// isFlushing:  false,
		}

		go bufferSizeCheckLoop()
	}

	// TODO : prepare Cassandra statements for common queries
	// Currently using session.bind() that registers statements if they don't exists

	return singleton, nil
}

func (h *Handler) Close() error {

	return nil
}

func (h *Handler) Set(cmd common.SetRequest) error {
	start := timer.Now()
	h.setbuffer <- CassandraSet{
		Key:     cmd.Key,
		Data:    cmd.Data,
		Flags:   cmd.Flags,
		Exptime: cmd.Exptime,
	}
	metrics.ObserveHist(HistSetL2BufferWait, timer.Since(start))
	return nil
}

func (h *Handler) Add(cmd common.SetRequest) error {

	return nil
}

func (h *Handler) Replace(cmd common.SetRequest) error {

	return nil
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
