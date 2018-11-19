// Implementation of a KeyValueServer. Students should write their code in this file.

package p0

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
)

type keyValueServer struct {
	clients  map[net.Conn]bool
	msg      chan CMD
	stop     chan interface{}
	deadConn chan net.Conn
	newConn  chan net.Conn
	count    chan chan int
	active   int
	dropped  int
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	return &keyValueServer{
		clients:  make(map[net.Conn]bool),
		msg:      make(chan CMD),
		newConn:  make(chan net.Conn),
		deadConn: make(chan net.Conn),
		stop:     make(chan interface{}),
		count:    make(chan chan int),
	}
}

func (kvs *keyValueServer) Start(port int) error {
	select {
	case <-kvs.stop:
		return fmt.Errorf("Cannot start server after its been started")
	default:
		break
	}
	init_db()

	listener, err := net.Listen("tcp4", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("problem starting : %v", err)
	}
	fmt.Println("Starting server on port :", port)

	// start server
	go func(l net.Listener) {
	accept:
		for {
			fmt.Println("Starting accept loop", l.Addr())
			select {
			case <-kvs.stop:
				fmt.Println("stopped")
				break accept
			default:
				fmt.Println("continue")
				break
			}
			fmt.Println("accepting")
			conn, err := l.Accept()

			if err != nil {
				fmt.Println("err")
				conn.Close()
				fmt.Println(err)
				continue
			}
			fmt.Println("accepting")
			client := NewClient(conn, kvs)
			go client.Start()
			kvs.newConn <- conn
		}
	}(listener)

	go func() {
	logic:
		for {
			select {
			case c := <-kvs.count:
				c <- len(kvs.clients)
			case conn := <-kvs.newConn:
				fmt.Println("New conn", conn)
				kvs.clients[conn] = true
			case <-kvs.stop:
				listener.Close()
				for client, _ := range kvs.clients {
					client.Close()
					kvs.dropped++
				}
				kvs.active = 0
				break logic
			case conn := <-kvs.deadConn:
				conn.Close()
				delete(kvs.clients, conn)
				kvs.dropped++
			case cmd := <-kvs.msg:
				cmd.Run()
			}
		}
	}()

	return nil
}

func (kvs *keyValueServer) Close() {
	// close (this returns the zero value everywhere immediately)
	close(kvs.stop)
}

func (kvs *keyValueServer) CountActive() int {
	c := make(chan int)
	kvs.count <- c
	return <-c

}

func (kvs *keyValueServer) CountDropped() int {
	return kvs.dropped
}

type client struct {
	stop     chan interface{}
	deadConn chan net.Conn
	write    chan fetchRes
	msgs     chan CMD
	conn     net.Conn
}

func NewClient(conn net.Conn, serv *keyValueServer) client {
	fmt.Println("new client", conn)
	return client{
		stop:     serv.stop,
		msgs:     serv.msg,
		deadConn: serv.deadConn,
		write:    make(chan fetchRes, 500),
		conn:     conn,
	}
}

func (cl client) Start() {
	fmt.Println("Starting client", cl.conn.RemoteAddr())
	go func() {
		for {
			msg, err := bufio.NewReader(cl.conn).ReadString('\n')
			if err != nil {
				fmt.Printf("err in client : %s", err)
				if err == io.EOF {
					cl.deadConn <- cl.conn
					break
				}
				continue
			}
			cmd, err := cl.parseMsg(msg)
			if err != nil {
				fmt.Printf("err in client : %s", err)
				continue
			}
			cl.msgs <- cmd
		}
	}()

loop:
	for {
		select {
		case msg := <-cl.write:
			for _, v := range msg.vals {
				n, err := fmt.Fprintf(cl.conn, "%s,%s\n", msg.k, v)
				if n == 0 || err != nil {
					fmt.Printf("err in client : %s", err)
					cl.deadConn <- cl.conn
					break loop
				}
			}
		case <-cl.stop:
			break loop
		}

	}
}

func (cl client) parseMsg(msg string) (CMD, error) {
	req := strings.Split(msg, ",")

	switch req[0] {
	case "get":
		return fetch{
			k:     req[1],
			reply: cl.write,
		}, nil
	case "put":
		return set{
			k: req[1],
			v: []byte(req[2]),
		}, nil
	case "delete":
		return del{
			k: req[1],
		}, nil
	default:
		return nil, fmt.Errorf("Invalid request : `%s`", msg)
	}
}

// CMD is sent by clients to the server
type CMD interface {
	Run()
}

type fetch struct {
	k     string
	reply chan fetchRes
}
type fetchRes struct {
	k    string
	vals [][]byte
}

func (f fetch) Run() {
	select {
	case f.reply <- fetchRes{f.k, get(f.k)}:
	default:
		return
	}
}

type set struct {
	k string
	v []byte
}

func (s set) Run() {
	put(s.k, s.v)
}

type del struct {
	k string
}

func (d del) Run() {
	clear(d.k)
}
