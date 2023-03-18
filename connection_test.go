package rabbitmq

import (
	"log"
	"testing"
	"time"
)

func TestConnection_Alive(t *testing.T) {
	conn, err := newConn()
	if err != nil {
		t.Fatal(err)
	}

	if !conn.IsAlive() {
		t.FailNow()
	}

	// restart rabbitmq server here manually
	log.Println("Restart rabbitmq server manually within 5 seconds")
	time.Sleep(5 * time.Second)

	if conn.IsAlive() {
		t.FailNow()
	}
}
