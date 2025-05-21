package fmqp

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
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

	reader := bufio.NewReader(conn)
	for {
		opCode, err := reader.ReadByte()
		if err != nil {
			if err != io.EOF {
				fmt.Printf("Read Error: %v \n", err)
			}
			return
		}

		var length uint32
		err = binary.Read(reader, binary.BigEndian, &length)
		if err != nil {
			if err != io.EOF {
				fmt.Printf("Read error: %v \n", err)
			}

		}

		//Read Payload
		payload := make([]byte, length)
		_, err = io.ReadFull(reader, payload)
		if err != nil {
			if err != io.EOF {
				fmt.Printf("Read error: %v \n", err)
			}

		}

		switch opCode {
		case 0x01: //ENQUEUE

		case 0x02: //DEQUEUE

		case 0x03: //ACK

		case 0x04: //NACK

		case 0x05: //RETRIECE DLQ

		case 0x06: //CLEAR DLQ

		default:
			//Write Error here
		}

	}

}
