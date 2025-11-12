# libsqlq – A Lightweight Queue Library with libsql Backend

`libsqlq` is a simple, generic, durable queue implementation in Go that uses **[libsql](https://github.com/tursodatabase/libsql)** (the open-source fork of SQLite) as its storage backend. It works both with local `.db` files and remote **Turso** databases, giving you a reliable, at-least-once message queue with minimal dependencies.

Perfect for background job processing, task queues, or any scenario where you need persistence without pulling in Redis, RabbitMQ, or other heavy infrastructure.

---

## Features

- **Generic** – `Queue[T any]` works with any JSON-serializable type.
- **Two deployment modes**:
  - Local file-based SQLite (`.db/<name>.db`)
  - Remote Turso (cloud-hosted libsql)
- **Built-in retry logic** with backoff (configurable)
- **Dead-letter behavior** – jobs exceeding `maxRetries` stay in the DB forever (manual cleanup possible)
- **At-least-once delivery** via claim-and-ack pattern
- **Single-writer safe** – uses transactions + row locks for concurrency

---

## Installation

```bash
go get github.com/shaneikennedy/libsqlq
```

> Requires Go 1.21+ (for generics)

Make sure you have the libsql driver:

```bash
go get github.com/tursodatabase/go-libsql
```

---

## Quick Start

### 1. Local Queue (file-based)

Checkout `examples/main.go` for a LocalQueue implementation.

### 2. Turso (remote) Queue

Set these environment variables:

```bash
export TURSO_URL="libsql://your-db.turso.io"
export TURSO_AUTH_TOKEN="your-auth-token"
# optional:
export TURSO_REMOTE_ENCRYPTION_KEY="your-key"
```

Then:

```go
q, err := libsqlq.NewTursoQueue[MyJob]()
if err != nil {
    log.Fatal(err)
}
```

Everything else works exactly the same.

---

## API Reference

### Creating a Queue

```go
// Local file-based
q, err := NewLocalQueue[MyPayload]("queue_name")

// Remote Turso
q, err := NewTursoQueue[MyPayload]()
```

### Configuration

```go
q = q.WithRetryBackoff(15 * time.Second)
q = q.WithMaxRetires(10)
```

### Enqueue

```go
err := q.Insert(MyPayload{...})
```

### Dequeue

```go
event, err := q.Next() // returns (*Event[T], error)
// event == nil → queue is empty
```

### Event

```go
type Event[T any] struct {
    Id      int
    Content *T   // pointer to deserialized payload
}
```

### Ack / Nack

```go
q.Ack(event.Id)   // remove permanently
q.Nack(event.Id)  // retry later after backoff
```

### Queue Size

```go
size, _ := q.Size() // only counts retry-eligible jobs
```
---

## Use Cases

- Background job processing
- Rate-limited API retries
- Webhook delivery queues
- Simple task scheduling
- Microservices that need durable messaging without extra infra

---

## Limitations & Gotchas

- **Not designed for millions of QPS** – libsql is SQLite under the hood.
- **At-least-once** only (duplicates possible if worker crashes after `Next()` but before `Ack()`).
- No built-in scheduling (delayed jobs) – use `Nack` + long backoff as workaround.
- Payload must be JSON-serializable.

---

**libsqlq** – Because sometimes all you need is a reliable SQLite queue. No brokers. No drama. Just `go run`./
