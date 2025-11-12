package queue

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log/slog"
	"os"
	"testing"
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
		t.Fail()
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
		t.Fail()
	}

	badData := Test{A: func(a string) { fmt.Println("hello from a failing test") }}
	err = q.Insert(badData)
	if err == nil {
		t.Fail()
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
		t.Fail()
	}

	data := Test{A: "hello from a passing test"}
	err = q.Insert(data)
	if err != nil {
		t.Fail()
	}
	if size, _ := q.Size(); size != 1 {
		t.Fail()
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
		t.Fail()
	}

	data := Test{A: "hello from a passing test"}
	err = q.Insert(data)
	if err != nil {
		t.Fail()
	}

	event, err := q.Next()
	if err != nil {
		t.Fail()
	}
	if event.Content.A != data.A {
		t.Fail()
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
		t.Fail()
	}

	data := Test{A: "hello from a passing test"}
	err = q.Insert(data)
	if err != nil {
		t.Fail()
	}

	event, err := q.Next()
	if err != nil {
		t.Fail()
	}
	err = q.Ack(event.Id)
	if err != nil {
		t.Fail()
	}
	if size, _ := q.Size(); size != 0 {
		t.Fail()
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
		t.Fail()
	}

	data := Test{A: "hello from a passing test"}
	err = q.Insert(data)
	if err != nil {
		t.Fail()
	}

	event, err := q.Next()
	if err != nil {
		t.Fail()
	}
	err = q.Nack(event.Id)
	if err != nil {
		t.Fail()
	}
	if size, _ := q.Size(); size != 1 {
		t.Fail()
	}
}
