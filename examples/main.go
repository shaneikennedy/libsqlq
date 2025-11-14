package main

import (
	"fmt"
	"libsqlq/queue"
	"math/rand"
	"time"
)

type HTTPResponse struct {
	Body string `json:"body"`
}

func main() {
	queue, err := queue.NewLocalQueue[HTTPResponse]("events")
	if err != nil {
		panic(err)
	}
	queue.WithRetryBackoffSeconds(5).WithMaxRetires(5).WithClaimTimeoutSeconds(1)
	for i := range 10 {
		err := queue.Insert(HTTPResponse{Body: fmt.Sprintf("Hello from event %d", i+1)})
		if err != nil {
			panic(err)
		}
	}

	for {
		if size, err := queue.Size(); size > 0 && err == nil {
			event, err := queue.Next()
			if err != nil {
				panic(err)
			}
			if event == nil {
				fmt.Println("No events available for pick up, sleeping and continuing")
				time.Sleep(2 * time.Second)
				continue
			}

			if rnd := rand.Float32(); rnd >= 0.8 {
				fmt.Printf("Acking event %d: %s \n", event.Id, event.Content.Body)
				err = queue.Ack(event.Id)
				if err != nil {
					fmt.Println("problem acking.... try again")
				}
			} else {
				fmt.Printf("Nacking event %d: %s \n", event.Id, event.Content.Body)
				err = queue.Nack(event.Id)
				if err != nil {
					fmt.Println("problem nacking.... try again")
				}
			}

		} else if err != nil {
			panic(err)
		} else {
			fmt.Println("Event queue now empty, exiting")
			break
		}
	}
	remainaing_events, err := queue.Size()
	if err != nil {
		panic(err)
	}

	if remainaing_events > 0 {
		panic(fmt.Sprintf("Something went wrong, the queue isn't empty. Remaining events: %d", remainaing_events))
	}

	fmt.Println("Event queue cleared!")

}
