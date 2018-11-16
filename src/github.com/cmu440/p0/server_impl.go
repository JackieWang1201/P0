// Implementation of a KeyValueServer. Students should write their code in this file.

package p0

import (
	"fmt"
	"net"
	"strings"
)

type keyValueServer struct {
	clients  map[net.Conn]bool
	msg      chan CMD
	stop     chan interface{}
	deadConn chan net.Conn
	newConn  chan net.Conn
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
	}
}

func (kvs *keyValueServer) Start(port int) error {
	// start server
	go func() {
		// accept loop
	}()
	// go accept loop
	// for select channels
	for {
		select {
		case conn := <-kvs.newConn:
			kvs.clients[conn] = true
			kvs.active++
		case <-kvs.stop:
		case conn := <-kvs.deadConn:
			delete(kvs.clients, conn)
			kvs.dropped++
		case msg := <-kvs.msg:
			msg.Run()
		}
	}
	return nil
}

func (kvs *keyValueServer) Close() {
	// close (this returns the zero value)
	close(kvs.stop)
}

func (kvs *keyValueServer) CountActive() int {
	return kvs.active
}

func (kvs *keyValueServer) CountDropped() int {
	return kvs.dropped
}

// TODO: add additional methods/functions below!
type client struct {
	stop  chan interface{}
	write chan [][]byte
	msgs  chan CMD
	conn  net.Conn
}

func (cl client) Start() error {
	// for select on write or stop
	// go read from conn and write to msgs or close
	return nil
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
		return delete{
			k: req[1],
		}, nil
	default:
		return nil, fmt.Errorf("Invalid request : `%s`", msg)
	}
}

// CMDs are sent by clients to the server
type CMD interface {
	Run()
}

type fetch struct {
	k     string
	reply chan [][]byte
}

func (f fetch) Run() {
	select {
	case f.reply <- get(f.k):
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

type delete struct {
	k string
}

func (d delete) Run() {
	clear(d.k)
}
