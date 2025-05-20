package fmqp

import (
	"fmt"
	"net"

	"github.com/FemeyCodes/femeycodes-MQ/internal/queue"
)

type Server struct {
	queue *queue.Queue
	addr  string
}

func NewServer(queue *queue.Queue, addr string) *Server {
	return &Server{
		queue: queue,
		addr:  addr,
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	fmt.Printf("Server listening on %s\n", s.addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Server listening on %v\n", err)
			continue
		}

		go s.handleClient(conn)

	}

}

func (s *Server) handleClient(conn net.Conn) {
	defer conn.Close()

}
