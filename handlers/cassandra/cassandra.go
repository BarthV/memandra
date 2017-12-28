package cassandra

import (
	"github.com/gocql/gocql"
	"github.com/netflix/rend/common"
	"github.com/netflix/rend/handlers"
)

type Handler struct {
	session *gocql.Session
}

var singleton *Handler

func New() (handlers.Handler, error) {
	clust := gocql.NewCluster("cassandra.host.service")
	clust.Keyspace = "kvstore"
	clust.Consistency = gocql.LocalOne
	sess, err := clust.CreateSession()
	if err != nil {
		return nil, err
	}

	// Only spawn a unique cassandra session per instance,
	// store this session in a global singleton.
	if singleton == nil {
		singleton = &Handler{
			session: sess,
		}
	}

	return singleton, nil
}

func (h *Handler) Close() error {

	return nil
}

func (h *Handler) Set(cmd common.SetRequest) error {

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

	var key gocql.UUID
	var val []byte

	if err := h.session.Query("select keycol,valuecol from kvtable where keycol=3761b107-552d-4eee-bbfb-4152fe1f7eca").Scan(&key, &val); err == nil {
		dataOut <- common.GetResponse{
			Miss:   false,
			Quiet:  cmd.Quiet[0],
			Opaque: cmd.Opaques[0],
			Flags:  0,
			Key:    []byte(key.String()),
			Data:   val,
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
