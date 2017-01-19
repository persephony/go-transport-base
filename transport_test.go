package transport

import (
	"net"
	"testing"
	"time"
)

type testListenerDialer struct {
	ln net.Listener
}

func newtestListenerDialer(addr string) (*testListenerDialer, error) {
	ln, err := net.Listen("tcp", addr)
	if err == nil {
		return &testListenerDialer{ln: ln}, nil
	}

	return nil, err
}

func (tld *testListenerDialer) Accept() (net.Conn, error) {
	return tld.ln.Accept()
}
func (tld *testListenerDialer) Close() error {
	return tld.ln.Close()
}
func (tld *testListenerDialer) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, timeout)
}
func (tld *testListenerDialer) Addr() net.Addr {
	return tld.ln.Addr()
}

func Test_TransportBase(t *testing.T) {
	mx, err := newtestListenerDialer("127.0.0.1:31415")
	if err != nil {
		t.Fatal(err)
	}

	tb := InitTransportBase(mx, 5*time.Second, 100*time.Second)
	tb.HandleConn = func(conn net.Conn) {
		for {
			b := make([]byte, 8)
			conn.Read(b)
			break
		}
	}
	go tb.Listen()

	<-time.After(1 * time.Second)

	c, err := tb.GetConn("127.0.0.1:31415")
	if err != nil {
		t.Fatal(err)
	}

	<-time.After(600 * time.Millisecond)

	if len(tb.inbound) != 1 {
		t.Error("should have 1 conn")
	}

	c.Write([]byte("foo"))

	<-time.After(1 * time.Second)

	if len(tb.inbound) != 0 {
		t.Errorf("should have 0 inbound conns: %d", len(tb.inbound))
	}
}
