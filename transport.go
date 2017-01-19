package transport

import (
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	mux "github.com/ipkg/go-mux"
)

type OutConn struct {
	net.Conn
	used time.Time
}

// TransportBase is a remote store
type TransportBase struct {
	sock *mux.Layer

	ilock   sync.RWMutex
	inbound map[net.Conn]struct{}

	olock    sync.Mutex            // connection pool lock
	outbound map[string][]*OutConn //conneciton pool

	dialTimeout time.Duration
	maxIdle     time.Duration

	shutdown int32

	// Call on each new connection in a separate go routine
	HandleConn func(net.Conn)
}

// InitTransportBase initializes the transport with an empty pool, starting the
// listener and connection reaper
func InitTransportBase(sock *mux.Layer, dialTimeout, maxIdle time.Duration) *TransportBase {
	cst := &TransportBase{
		sock:        sock,
		outbound:    map[string][]*OutConn{},
		inbound:     map[net.Conn]struct{}{},
		dialTimeout: dialTimeout,
		maxIdle:     maxIdle,
	}
	return cst
}

// Listen starts listening for connections and starts the reaping of old ones.
func (st *TransportBase) Listen() {
	go st.listen()
	go st.reapOld()
}

func (st *TransportBase) listen() {
	for {
		conn, err := st.sock.Accept()
		if err != nil {
			log.Println("ERR", err)
			continue
		}

		st.ilock.Lock()
		st.inbound[conn] = struct{}{}
		st.ilock.Unlock()

		go st.handleConn(conn)
	}
}
func (st *TransportBase) handleConn(conn net.Conn) {
	defer func() {
		st.ilock.Lock()
		delete(st.inbound, conn)
		st.ilock.Unlock()
		conn.Close()
	}()

	st.HandleConn(conn)
}

// Closes old outbound connections
func (st *TransportBase) reapOld() {
	for {
		if atomic.LoadInt32(&st.shutdown) == 1 {
			return
		}
		time.Sleep(30 * time.Second)
		st.reapOnce()
	}
}

func (st *TransportBase) reapOnce() {
	st.olock.Lock()
	defer st.olock.Unlock()

	for host, conns := range st.outbound {
		max := len(conns)
		for i := 0; i < max; i++ {
			if time.Since(conns[i].used) > st.maxIdle {
				conns[i].Close()
				conns[i], conns[max-1] = conns[max-1], nil
				max--
				i--
			}
		}
		// Trim any idle conns
		st.outbound[host] = conns[:max]
	}
}

// Shutdown the store transport
func (st *TransportBase) Shutdown() error {
	atomic.StoreInt32(&st.shutdown, 1)
	st.sock.Close()

	// Close all the inbound connections
	st.ilock.RLock()
	for conn := range st.inbound {
		conn.Close()
	}
	st.ilock.RUnlock()

	// Close all the outbound
	st.olock.Lock()
	for _, conns := range st.outbound {
		for _, out := range conns {
			out.Close()
		}
	}
	st.outbound = nil
	st.olock.Unlock()

	return nil
}

// GetConn from pool or create a new one
func (st *TransportBase) GetConn(host string) (*OutConn, error) {
	// Check if we have a conn cached
	var out *OutConn
	st.olock.Lock()
	if atomic.LoadInt32(&st.shutdown) == 1 {
		st.olock.Unlock()
		return nil, fmt.Errorf("transport is shutdown")
	}

	list, ok := st.outbound[host]
	if ok && len(list) > 0 {
		out = list[len(list)-1]
		list = list[:len(list)-1]
		st.outbound[host] = list
	}
	st.olock.Unlock()
	// Make a new connection
	if out == nil {
		conn, err := st.sock.Dial(host, st.dialTimeout)
		//conn, err := grpc.Dial(host, grpc.WithInsecure())
		if err == nil {
			return &OutConn{used: time.Now(), Conn: conn}, nil
		}
		return nil, err
	}
	// return an existing connection
	return out, nil
}

// ReturnConn back to the pool
func (st *TransportBase) ReturnConn(c *OutConn) {
	c.used = time.Now()

	st.olock.Lock()
	defer st.olock.Unlock()

	if atomic.LoadInt32(&st.shutdown) == 1 {
		c.Close()
		return
	}

	// Push back into the pool
	list, _ := st.outbound[c.RemoteAddr().String()]
	st.outbound[c.RemoteAddr().String()] = append(list, c)
}
