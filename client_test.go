package elastilog_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/treetopllc/elastilog"
)

func TestClient(t *testing.T) {
	c := elastilog.NewClient("http://localhost:9200", "noblehack", "elastic")
	count := 100
	sleep := time.Millisecond * 20
	start := time.Now()
	for i := 0; i < count; i++ {
		c.Send(elastilog.Entry{
			Timestamp: time.Now(),
			Host:      "debian-jessie",
			Log:       "SLOW:" + strings.Repeat("Noble\n", i%5+1),
			Attributes: elastilog.Attributes{
				"request.url":     "http://foo.bar.com/baz/?data=" + strings.Repeat("data", i%3+1),
				"response.status": (i%4 + 2) * 100,
				"service":         "UsersService",
			},
		})
		time.Sleep(sleep)
	}
	end := time.Now()
	fmt.Printf("Took %v per\n", (end.Sub(start))/time.Duration(count)-sleep)
	fmt.Println("Done sending, waiting on close")
	start = time.Now()
	c.Close()
	end = time.Now()
	fmt.Printf("Took %v close\n", end.Sub(start))
}
