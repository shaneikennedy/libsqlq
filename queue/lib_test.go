package queue

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"
)

func randomString(n int) string {
	// n bytes → ~1.33 * n base64 chars
	data := make([]byte, n)
	if _, err := rand.Read(data); err != nil {
		panic(err)
	}
	return base64.URLEncoding.EncodeToString(data)[:n]
}

func TestNewLocalQueue(t *testing.T) {
	type Test struct{}
	q, err := NewLocalQueue[Test](randomString(10))
	defer func() {
		err := os.Remove(q.Location())
		if err != nil {
			slog.Error(fmt.Sprintf("Unable to remove db at location: %s", q.Location()))
		}
		err = os.Remove(".db")
		if err != nil {
			slog.Error("Unable to remove .db dir")
		}
	}()
	if err != nil {
		fmt.Printf("Failing with %v\n", err)
		t.Fatal()
	}
}

func TestInsertFailsIfNotJsonSerializable(t *testing.T) {
	// A function is not json serializable
	type Test struct{ A func(string) }
	q, err := NewLocalQueue[Test](randomString(10))
	defer func() {
		err := os.Remove(q.Location())
		if err != nil {
			slog.Error(fmt.Sprintf("Unable to remove db at location: %s", q.Location()))
		}
		err = os.Remove(".db")
		if err != nil {
			slog.Error("Unable to remove .db dir")
		}
	}()
	if err != nil {
		fmt.Printf("Failing with %v\n", err)
		t.Fatal()
	}

	badData := Test{A: func(a string) { fmt.Println("hello from a failing test") }}
	err = q.Insert(badData)
	if err == nil {
		t.Fatal()
	}

}

func TestInsert(t *testing.T) {
	type Test struct{ A string }
	q, err := NewLocalQueue[Test](randomString(10))
	defer func() {
		err := os.Remove(q.Location())
		if err != nil {
			slog.Error(fmt.Sprintf("Unable to remove db at location: %s", q.Location()))
		}
		err = os.Remove(".db")
		if err != nil {
			slog.Error("Unable to remove .db dir")
		}
	}()
	if err != nil {
		fmt.Printf("Failing with %v\n", err)
		t.Fatal()
	}

	data := Test{A: "hello from a passing test"}
	err = q.Insert(data)
	if err != nil {
		t.Fatal()
	}
	if size, _ := q.Size(); size != 1 {
		t.Fatal()
	}

}

func TestNext(t *testing.T) {
	type Test struct{ A string }
	q, err := NewLocalQueue[Test](randomString(10))
	defer func() {
		err := os.Remove(q.Location())
		if err != nil {
			slog.Error(fmt.Sprintf("Unable to remove db at location: %s", q.Location()))
		}
		err = os.Remove(".db")
		if err != nil {
			slog.Error("Unable to remove .db dir")
		}
	}()
	if err != nil {
		fmt.Printf("Failing with %v\n", err)
		t.Fatal()
	}

	data := Test{A: "hello from a passing test"}
	err = q.Insert(data)
	if err != nil {
		t.Fatal()
	}

	event, err := q.Next()
	if err != nil {
		t.Fatal()
	}
	if event.Content.A != data.A {
		t.Fatal()
	}
}

func TestAck(t *testing.T) {
	type Test struct{ A string }
	q, err := NewLocalQueue[Test](randomString(10))
	defer func() {
		err := os.Remove(q.Location())
		if err != nil {
			slog.Error(fmt.Sprintf("Unable to remove db at location: %s", q.Location()))
		}
		err = os.Remove(".db")
		if err != nil {
			slog.Error("Unable to remove .db dir")
		}
	}()
	if err != nil {
		fmt.Printf("Failing with %v\n", err)
		t.Fatal()
	}

	data := Test{A: "hello from a passing test"}
	err = q.Insert(data)
	if err != nil {
		t.Fatal()
	}

	event, err := q.Next()
	if err != nil {
		t.Fatal()
	}
	err = q.Ack(event.Id)
	if err != nil {
		t.Fatal()
	}
	if size, _ := q.Size(); size != 0 {
		t.Fatal()
	}
}

func TestNack(t *testing.T) {
	type Test struct{ A string }
	q, err := NewLocalQueue[Test](randomString(10))
	defer func() {
		err := os.Remove(q.Location())
		if err != nil {
			slog.Error(fmt.Sprintf("Unable to remove db at location: %s", q.Location()))
		}
		err = os.Remove(".db")
		if err != nil {
			slog.Error("Unable to remove .db dir")
		}
	}()
	if err != nil {
		fmt.Printf("Failing with %v\n", err)
		t.Fatal()
	}

	data := Test{A: "hello from a passing test"}
	err = q.Insert(data)
	if err != nil {
		t.Fatal()
	}

	event, err := q.Next()
	if err != nil {
		t.Fatal()
	}
	err = q.Nack(event.Id)
	if err != nil {
		t.Fatal()
	}
	if size, _ := q.Size(); size != 1 {
		t.Fatal()
	}
}

func TestClaimTimeout(t *testing.T) {
	type Test struct{ A string }
	q, err := NewLocalQueue[Test](randomString(10))
	q = q.WithClaimTimeoutSeconds(1)
	defer func() {
		err := os.Remove(q.Location())
		if err != nil {
			slog.Error(fmt.Sprintf("Unable to remove db at location: %s", q.Location()))
		}
		err = os.Remove(".db")
		if err != nil {
			slog.Error("Unable to remove .db dir")
		}
	}()
	if err != nil {
		fmt.Printf("Failing with %v\n", err)
		t.Fatal()
	}

	data := Test{A: "hello from a passing test"}
	err = q.Insert(data)
	if err != nil {
		t.Fatal()
	}

	event, err := q.Next()
	if err != nil {
		t.Fatal()
	}
	if event == nil {
		t.Fatal()
	}
	to_be_reclaimed_id := event.Id
	// Only one event placed in the queue, make sure Next() returns nil event
	// Otherwiser the rest of the test doesn't make sense
	expected_nil_event, err := q.Next()
	if err != nil || expected_nil_event != nil {
		t.Fatal()
	}

	// Don't ack the event, just hold it past the configured claim timeout
	time.Sleep(4 * time.Second)

	reclaimed_event, err := q.Next()
	if err != nil {
		t.Fatal()
	}
	if reclaimed_event == nil {
		// This means the exipiration/cleanup didn't work
		t.Fatal()
	}
	if reclaimed_event.Id != to_be_reclaimed_id {
		t.Fatal()
	}
}
