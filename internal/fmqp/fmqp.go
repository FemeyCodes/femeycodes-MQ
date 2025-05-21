package fmqp

import (
	"bufio"
	"bytes"
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
			s.writeError(conn, fmt.Sprintf("Cannot identify type of request, Unknown opCode recieved: %x ", opCode))
		}

	}

}

func (s *Server) writeError(conn net.Conn, msg string) {
	payload := []byte(msg)
	conn.Write([]byte{0xFF})
	binary.Write(conn, binary.BigEndian, payload)
	conn.Write(payload)
}

func (s *Server) writeSuccess(conn net.Conn, payload []byte) {
	conn.Write([]byte{0x00})
	binary.Write(conn, binary.BigEndian, uint32(len(payload)))
	conn.Write(payload)
}

func (s *Server) handleEnqueue(conn net.Conn, payload []byte) {
	reader := bytes.NewReader(payload)
	var payloadLen uint32
	err := binary.Read(reader, binary.BigEndian, &payloadLen)
	if err != nil {
		s.writeError(conn, "invalid payload length")
		return
	}

	payLoadData := make([]byte, payloadLen)
	_, err = io.ReadFull(reader, payLoadData)
	if err != nil {
		s.writeError(conn, "invalid payload data")
		return
	}

	var priority int32
	err = binary.Read(reader, binary.BigEndian, priority)
	if err != nil {
		s.writeError(conn, "invalid priority received")
		return
	}

	var metadataCount uint32
	if err := binary.Read(reader, binary.BigEndian, &metadataCount); err != nil {
		s.writeError(conn, "invalid metadata count")
		return
	}
	metadata := make(map[string]string)
	for i := uint32(0); i < metadataCount; i++ {
		var keyLen uint32
		if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
			s.writeError(conn, "invalid key length")
			return
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, key); err != nil {
			s.writeError(conn, "invalid key data")
			return
		}
		var valueLen uint32
		if err := binary.Read(reader, binary.BigEndian, &valueLen); err != nil {
			s.writeError(conn, "invalid value length")
			return
		}
		value := make([]byte, valueLen)
		if _, err := io.ReadFull(reader, value); err != nil {
			s.writeError(conn, "invalid value data")
			return
		}
		metadata[string(key)] = string(value)
	}

	s.queue.Enqueue(payLoadData, priority, metadata)
	s.writeSuccess(conn, nil)

}
