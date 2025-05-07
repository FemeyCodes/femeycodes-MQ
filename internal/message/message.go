package message

import "time"

type Message struct {
	Priority   int32
	RetryCount int32
	ID         string
	Metadata   map[string]string
	Payload    []byte
	Timestamp  time.Time
}

func (m *Message) ResetRetryCount() {
	m.RetryCount = 0

}
