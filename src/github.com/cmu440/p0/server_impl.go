// Implementation of a KeyValueServer. Students should write their code in this file.

package p0

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

type keyValueServer struct {
	clients      map[net.Conn]bool
	msg          chan CMD
	stop         chan interface{}
	deadConn     chan net.Conn
	newConn      chan net.Conn
	countActive  chan chan int
	countDropped chan chan int
	active       int
	dropped      int
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {
	return &keyValueServer{
		clients:      make(map[net.Conn]bool),
		msg:          make(chan CMD),
		newConn:      make(chan net.Conn),
		deadConn:     make(chan net.Conn),
		stop:         make(chan interface{}),
		countActive:  make(chan chan int),
		countDropped: make(chan chan int),
	}
}

func (kvs *keyValueServer) Start(port int) error {
	select {
	case <-kvs.stop:
		return fmt.Errorf("Cannot start server after its been started")
	default:
		init_db()
		break
	}

	listener, err := net.Listen("tcp4", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("problem starting : %v", err)
	}
	fmt.Println("Starting server on port :", port)

	// start server
	go func(l net.Listener) {
	accept:
		for {
			select {
			case <-kvs.stop:
				fmt.Println("stopped")
				break accept
			default:
				break
			}
			conn, err := l.Accept()
			if err != nil {
				break accept
			}
			client := NewClient(conn, kvs)
			go client.Start()
			kvs.newConn <- conn
		}
	}(listener)

	go func() {
	logic:
		for {
			select {
			case c := <-kvs.countActive:
				c <- kvs.active
			case c := <-kvs.countDropped:
				c <- kvs.dropped
			case conn := <-kvs.newConn:
				kvs.clients[conn] = true
				kvs.active++
			case <-kvs.stop:
				listener.Close()
				for client, _ := range kvs.clients {
					client.Close()
					delete(kvs.clients, client)
					kvs.dropped++
				}
				kvs.active = 0
				break logic
			case conn := <-kvs.deadConn:
				conn.Close()
				delete(kvs.clients, conn)
				kvs.active--
				kvs.dropped++
			case cmd := <-kvs.msg:
				cmd.Run()
			}
		}
	}()

	return nil
}

func (kvs *keyValueServer) Close() {
	close(kvs.stop)
}

func (kvs *keyValueServer) CountActive() int {
	c := make(chan int)
	kvs.countActive <- c
	return <-c
}

func (kvs *keyValueServer) CountDropped() int {
	c := make(chan int)
	kvs.countDropped <- c
	return <-c
}

type client struct {
	stop     chan interface{}
	deadConn chan net.Conn
	dead     chan interface{}
	write    chan fetchRes
	msgs     chan CMD
	conn     net.Conn
}

func NewClient(conn net.Conn, serv *keyValueServer) client {
	return client{
		stop:     serv.stop,
		msgs:     serv.msg,
		dead:     make(chan interface{}),
		deadConn: serv.deadConn,
		write:    make(chan fetchRes, 500),
		conn:     conn,
	}
}

func (cl client) Start() {
	go func() {
		for {
			select {
			case <-cl.dead:
				return
			default:
				break
			}
			scanner := bufio.NewScanner(cl.conn)
			for {
				scanned := scanner.Scan()
				if !scanned {
					close(cl.dead)
					return
				}

				cmd, err := cl.parseMsg(scanner.Text())
				if err != nil {
					fmt.Printf("err parsing : %s\n", err)
					continue
				}
				cl.msgs <- cmd
			}
		}
	}()

loop:
	for {
		select {
		case msg := <-cl.write:
		vals:
			for _, v := range msg.vals {
				n, err := fmt.Fprintf(cl.conn, "%s,%s\n", msg.k, v)
				if n == 0 || err != nil {
					fmt.Printf("err writing : %s\n", err)
					close(cl.dead)
					break vals
				}
			}
		case <-cl.dead:
			cl.conn.Close()
			cl.deadConn <- cl.conn
			break loop
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
	case f.reply <- fetchRes{k: f.k, vals: get(f.k)}:
	default:
		return
	}
}

type set struct {
	k string
	v []byte
}

func (s set) Run() {
	// fmt.Println("set", s)
	put(s.k, s.v)
}

type del struct {
	k string
}

func (d del) Run() {
	clear(d.k)
}
