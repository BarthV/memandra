package redis

import (
	"time"

	"github.com/go-redis/redis"
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
)

type Handler struct {
	client *redis.Client
}

type RedisSet struct {
	Key     []byte
	Data    []byte
	Flags   uint32
	Exptime uint32
}

// Batching metrics
var (
// MetricSetBufferSize      = metrics.AddIntGauge("cmd_set_batch_buffer_size", nil)
)

var singleton *Handler

// InitRedisConn initialize Cassandra global connection, call it once before starting ListenAndServe()
func InitRedisConn() error {
	// Only spawn a unique cassandra session,
	// store this session in a global singleton.
	if singleton == nil {
		client := redis.NewClient(&redis.Options{
			Network:  "unix",
			Addr:     "/tmp/redis.sock",
			Password: "", // no password set
			DB:       0,  // use default DB
		})
		_, err := client.Ping().Result()
		if err != nil {
			return err
		}

		singleton = &Handler{
			client: client,
		}
	}
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
	realExptime := time.Duration(computeExpTime(cmd.Exptime)) * time.Second

	err := h.client.Set(string(cmd.Key), string(cmd.Data), realExptime).Err()
	if err != nil {
		return common.ErrInternal
	}

	// TODO : maybe add a set timeout that return "not_stored" in case of buffer error ?
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
		val, err := h.client.Get(string(key)).Result()
		if err == nil {
			dataOut <- common.GetResponse{
				Miss:   false,
				Quiet:  cmd.Quiet[idx],
				Opaque: cmd.Opaques[idx],
				Flags:  0,
				Key:    key,
				Data:   []byte(val),
			}
		} else {
			dataOut <- common.GetResponse{
				Miss:   true,
				Quiet:  cmd.Quiet[idx],
				Opaque: cmd.Opaques[idx],
				Key:    key,
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

	return nil
}

func (h *Handler) Touch(cmd common.TouchRequest) error {

	return nil
}
