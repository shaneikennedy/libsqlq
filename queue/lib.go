package queue

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"time"

	_ "github.com/tursodatabase/go-libsql"
)

type Queue[T any] struct {
	db                  *sql.DB
	retryBackoffSeconds int
	maxRetries          int
	location            string
	claimTimeoutSeconds int
	lock                sync.RWMutex
}

type Event[T any] struct {
	Id      int
	Content *T
}

const CREATE_TABLE_STATEMENT = `CREATE TABLE IF NOT EXISTS queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    payload TEXT NOT NULL,
    enqueued_at TEXT DEFAULT (datetime('now', 'utc')),
    claimed INTEGER DEFAULT 0,           -- 1 = being processed
    claim_expires TEXT,                 -- ISO string
    retries INTEGER DEFAULT 0
);
`

const CREATE_UNCLAIMED_INDEX_STATEMENT = `CREATE INDEX IF NOT EXISTS idx_unclaimed ON queue (id) WHERE claimed = 0;`

// Creates a new libsql database called "<name>.db" in $(cwd)/.db
// Or loads an existing one.
// The queue is generic for type T, which mush be json-serializable
// If type T is not json serailizable attempts to insert jobs will fail
// The database is persisted on the filesystem and if a new process
// Attempts to create a queue with the name name, the backing libsql database will be reused.
// A default retry_backoff is configured at 5s and a maximum retries of 1000
func NewLocalQueue[T any](name string) (*Queue[T], error) {
	// Create a .db dir if it doesn't already exists
	if err := os.MkdirAll(".db", 0775); err != nil {
		return nil, err
	}
	dbUrl := "file:.db/" + name + ".db"
	return newQueueWithDefaults[T](dbUrl)
}

// Creates a new libsql database called "<name>.db" in $(cwd)/.db
// Or loads an existing one
// The queue is generic for type T, which mush be json-serializable
// If type T is not json serailizable attempts to insert jobs will fail
// The database is persisted on the filesystem and if a new process
// Attempts to create a queue with the name name, the backing libsql database will be reused.
// A default retry_backoff is configured at 5s and a maximum retries of 1000
func NewTursoQueue[T any]() (*Queue[T], error) {
	// Get database URL and auth token from environment variables
	dbUrl := os.Getenv("TURSO_URL")
	if dbUrl == "" {
		return nil, fmt.Errorf("TURSO_URL environment variable not set")
	}

	sep := "?"
	authToken := os.Getenv("TURSO_AUTH_TOKEN")
	if authToken != "" {
		dbUrl += sep + "authToken=" + authToken
		sep = "&"
	}

	remoteEncryptionKey := os.Getenv("TURSO_REMOTE_ENCRYPTION_KEY")
	if remoteEncryptionKey != "" {
		dbUrl += sep + "remoteEncryptionKey=" + remoteEncryptionKey
	}
	return newQueueWithDefaults[T](dbUrl)
}

func newQueueWithDefaults[T any](dbUrl string) (*Queue[T], error) {
	db, err := sql.Open("libsql", dbUrl)
	if err != nil {
		return nil, err

	}
	_, err = db.Exec(CREATE_TABLE_STATEMENT)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(CREATE_UNCLAIMED_INDEX_STATEMENT)
	if err != nil {
		return nil, err
	}

	queue := &Queue[T]{
		db:                  db,
		retryBackoffSeconds: 5,
		maxRetries:          1000,
		location:            dbUrl,
		claimTimeoutSeconds: 30,
	}

	go queue.startClaimTimeoutCleanup()

	return queue, nil
}

const CLAIM_TIMEOUT_CLEANUP_QUERY = `
UPDATE queue
SET claimed = 0, claim_expires = NULL
WHERE claimed = 1
AND (claim_expires IS NOT NULL AND claim_expires < datetime('now', 'utc'))
RETURNING id
`

// Technically not needed based on how the claim query works
// But this is inexpensive and makes debugging state easier
func (q *Queue[T]) startClaimTimeoutCleanup() {

	for {
		q.lock.Lock()
		reclaimed_jobs, err := q.db.Query(CLAIM_TIMEOUT_CLEANUP_QUERY)
		q.lock.Unlock()
		if err != nil {
			slog.Error(fmt.Errorf("problem reclaiming jobs from queue after claimTimeout has expired: %w", err).Error())
			err = reclaimed_jobs.Close()
			if err != nil {
				slog.Error(fmt.Errorf("problem closing the reclaimed_jobs pointer: %w", err).Error())
			}
			time.Sleep(time.Duration(q.claimTimeoutSeconds) * time.Second)
			continue
		}
		if reclaimed_jobs == nil {
			slog.Info("No jobs to reclaim")
			err = reclaimed_jobs.Close()
			if err != nil {
				slog.Error(fmt.Errorf("problem closing the reclaimed_jobs pointer: %w", err).Error())
			}
			time.Sleep(time.Duration(q.claimTimeoutSeconds) * time.Second)
			continue
		}
		for reclaimed_jobs.Next() {
			var id int
			err = reclaimed_jobs.Scan(&id)
			if err != nil {
				slog.Error(fmt.Errorf("problem scanning a reclaimed row: %w", err).Error())
			}
			slog.Info(fmt.Sprintf("Reclaimed event after claim timeout expiration: %d", id))

		}
		err = reclaimed_jobs.Close()
		if err != nil {
			slog.Error(fmt.Errorf("problem closing the reclaimed_jobs pointer: %w", err).Error())
		}
		time.Sleep(time.Duration(q.claimTimeoutSeconds) * time.Second)
	}
}

// Configure the retry backoff for the queue, i.e how long after a failure
// Before an event can be retried
func (q *Queue[T]) WithRetryBackoffSeconds(backoff int) *Queue[T] {
	q.retryBackoffSeconds = backoff
	return q
}

// Configure the maximum number of retires for an event. The event will not be cleaned up from the database, making this effectively a Dead-Letter Queue.
func (q *Queue[T]) WithMaxRetires(max int) *Queue[T] {
	q.maxRetries = max
	return q
}

// Configure how long a process has to process an event before it is made available to be consumed by other processes
func (q *Queue[T]) WithClaimTimeoutSeconds(timeout int) *Queue[T] {
	q.claimTimeoutSeconds = timeout
	return q
}

const INSERT_QUERY_TEMPLATE = `INSERT INTO queue (payload) VALUES ('%s')`

// Insert an event of type T. This will create an Event with an id field, and the json-serailized
// string of payload
func (q *Queue[T]) Insert(payload T) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("unable to marshal data of type %T to json: %w", payload, err)
	}

	q.lock.Lock()
	defer q.lock.Unlock()
	_, err = q.db.Exec(fmt.Sprintf(INSERT_QUERY_TEMPLATE, data))
	if err != nil {
		return fmt.Errorf("problem inserting event to queue: %w", err)
	}
	return nil
}

const NEXT_JOB_TEMPLATE = `
SELECT id FROM queue
WHERE claimed = 0
AND (claim_expires <= datetime('now', 'utc') OR claim_expires IS NULL)
AND retries <= :max_retires
ORDER BY id ASC LIMIT 1
`

const CLAIM_JOB_QUERY_TEMPLATE = `
UPDATE queue
SET claimed = 1,
claim_expires = datetime('now', printf('+%d seconds', ?), 'utc')
WHERE id = ?
AND (claimed = 0 OR claim_expires IS NULL OR claim_expires <= datetime('now', 'utc'))
RETURNING id, payload
`

// Return the "next" event in the queue, that is, returns the oldest event
// that was submitted that is not already being processed and is not in the
// configured retry backoff period
func (q *Queue[T]) Next() (*Event[T], error) {
	q.lock.Lock()
	defer q.lock.Unlock()
	tx, err := q.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("problem starting transaction on db %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
			slog.Error(fmt.Sprintf("WARNING: tx.Rollback() failed: %v\n", err))
		}
	}()
	var candidate int
	err = tx.QueryRow(NEXT_JOB_TEMPLATE, sql.Named("max_retires", q.maxRetries)).Scan(&candidate)
	if err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("problem getting next event in queue: %w", err)
	}
	var id int
	var data string
	err = tx.QueryRow(CLAIM_JOB_QUERY_TEMPLATE, q.claimTimeoutSeconds, candidate).Scan(&id, &data)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("it's ehre %w", err)
	} else if err != nil {
		return nil, fmt.Errorf("problem claiming event from queue: %w", err)
	}
	var payload T
	err = json.Unmarshal([]byte(data), &payload)
	if err != nil {
		return nil, fmt.Errorf("problem unmarshalling data from queue to type %T: %w", payload, err)
	}
	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("promblem commiting transaction when attempting to claim item from queue: %w", err)
	}
	return &Event[T]{id, &payload}, nil
}

const ACK_QUERY_TEMPLATE = `DELETE FROM queue WHERE id = %d`

// Ackknowledge the successful processing of event with id: id. Once acked, this event
// Is removed from the database and will not be processed again
func (q *Queue[T]) Ack(id int) error {
	q.lock.Lock()
	defer q.lock.Unlock()
	_, err := q.db.Exec(fmt.Sprintf(ACK_QUERY_TEMPLATE, id))
	if err != nil {
		return fmt.Errorf("unable to ack event: %d: %w", id, err)
	}
	return nil
}

const NACK_QUERY_TEMPLATE = `UPDATE queue SET retries = retries + 1, claimed = 0, claim_expires = datetime('now', printf('+%d seconds', ?), 'utc') WHERE id = ?`

// Negative Ack indicates that the event with id: id was not able to be processed, and will be put in quarantice
// for the configured backoff period before being available to be de-queued again
func (q *Queue[T]) Nack(id int) error {
	jitter := rand.Intn(3)
	q.lock.Lock()
	defer q.lock.Unlock()
	_, err := q.db.Query(NACK_QUERY_TEMPLATE, q.retryBackoffSeconds+jitter, id)
	if err != nil {
		return fmt.Errorf("unable to nack event: %d: %w", id, err)
	}
	return nil
}

const QUEUE_SIZE_TEMPLATE = `SELECT COUNT(*) from queue where retries <= :max_retries;`

// Returns the number of events in the queue
func (q *Queue[T]) Size() (int, error) {
	var size int
	q.lock.RLock()
	defer q.lock.RUnlock()
	err := q.db.QueryRow(QUEUE_SIZE_TEMPLATE, sql.Named("max_retries", q.maxRetries)).Scan(&size)
	if err != nil {
		return -1, fmt.Errorf("problem getting number of events in the queue: %w", err)
	}
	return size, nil
}

// Where the db is stored. This returns a string that may be a path or a turso connection url
// Depending on what type of queue was instantiated
func (q *Queue[T]) Location() string {
	return q.location
}
