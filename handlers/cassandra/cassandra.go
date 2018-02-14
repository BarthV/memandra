package cassandra

import (
	"time"

	"github.com/gocql/gocql"
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
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

var singleton *Handler

func unsetFlushingState() {
	// singleton.isFlushing = false
}

func bufferSizeCheckLoop() {
	ticker := time.NewTicker(5 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			if len(singleton.setbuffer) >= 1000 {
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

	if chanLen > 0 {
		// fmt.Println(chanLen)
		if chanLen >= 5000 {
			chanLen = 5000
		}
		b := singleton.session.NewBatch(gocql.UnloggedBatch)
		for i := 1; i <= chanLen; i++ {
			item := (<-singleton.setbuffer)
			b.Query("INSERT INTO kvstore.bucket1 (keycol,valuecol) VALUES (?, ?) USING TTL ?", item.Key, item.Data, item.Exptime)
		}
		// exec CQL batch
		singleton.session.ExecuteBatch(b)
	}
	singleton.buffertimer.Reset(200 * time.Millisecond)
}

func New() (handlers.Handler, error) {
	// Only spawn a unique cassandra session per instance,
	// store this session in a global singleton.
	if singleton == nil {
		clust := gocql.NewCluster("10.228.14.38")
		clust.Keyspace = "kvstore"
		clust.Consistency = gocql.LocalOne
		sess, err := clust.CreateSession()
		if err != nil {
			return nil, err
		}

		singleton = &Handler{
			session:     sess,
			setbuffer:   make(chan CassandraSet, 80000),
			buffertimer: time.AfterFunc(200*time.Millisecond, flushBuffer),
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
	h.setbuffer <- CassandraSet{
		Key:     cmd.Key,
		Data:    cmd.Data,
		Flags:   cmd.Flags,
		Exptime: cmd.Exptime,
	}
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

		if err := h.session.Bind("SELECT keycol,valuecol FROM kvstore.bucket1 where keycol=?", key_qi).Scan(&key, &val); err == nil {
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

		if err := h.session.Bind("SELECT keycol,valuecol,TTL(valuecol) FROM kvstore.bucket1 where keycol=?", key_qi).Scan(&key, &val, &ttl); err == nil {
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

	if err := h.session.Bind("DELETE FROM kvstore.bucket1 WHERE keycol=?", kv_qi).Exec(); err != nil {
		return err
	}
	return nil
}

func (h *Handler) Touch(cmd common.TouchRequest) error {

	return nil
}
