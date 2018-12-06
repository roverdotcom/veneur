package main

import (
	"context"
	"flag"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stripe/veneur/metricingester"
	"golang.org/x/time/rate"
)

func main() {
	qps := flag.Int("qps", 100, "Queries per second to make.")
	hostport := flag.String("hostport", "localhost:8200", "hostport to send requests to")
	parallelism := flag.Int("parallelism", runtime.NumCPU()*2, "number of workers to generate load")
	flag.Parse()

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// START LOAD GENERATOR

	vl := veneurLoadGenerator{
		f: metricingester.NewForwardingIngester(newSingleConnector(*parallelism, *hostport)),
		m: metricingester.NewCounter("test", 10, []string{"abc:def"}, 1.0, "hostname"),
	}
	loader := newLoader(*qps, vl.run, *parallelism)
	loader.Start()
	start := time.Now()
	log.Printf("STARTING // requesting %s at %v qps", *hostport, *qps)

	// HANDLE SIGINT

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	cleanupDone := make(chan struct{})
	go func() {
		<-signalChan
		dur := time.Now().Sub(start)
		log.Fatalf(
			"FINISHED // made %d requests in %s // achieved %d qps",
			*loader.requestCount,
			dur,
			*loader.requestCount/uint64(math.Round(dur.Seconds())),
		)
		close(cleanupDone)
	}()
	<-cleanupDone
}

/*
 LOAD GENERATOR
*/

type loader struct {
	cb          func()
	limiter     *rate.Limiter
	parallelism int

	requestCount *uint64
	stopped      bool
}

func newLoader(qps int, cb func(), parallelism int) loader {
	return loader{
		cb:           cb,
		limiter:      rate.NewLimiter(rate.Limit(qps), qps),
		parallelism:  parallelism,
		requestCount: new(uint64),
	}
}

func (l *loader) Start() {
	for i := 0; i < l.parallelism; i++ {
		go l.runner()
	}
}

func (l *loader) runner() {
	for {
		if err := l.limiter.Wait(context.Background()); err != nil {
			log.Fatalf("error getting rate limit: %v", err)
		}
		l.cb()
		atomic.AddUint64(l.requestCount, 1)
	}
}

/*
 * VENEUR CONNECTION GENERATOR
 */
type singleConnector struct {
	conns    []net.Conn
	randPool sync.Pool
}

func newSingleConnector(nConns int, hostport string) singleConnector {
	randPool := sync.Pool{New: func() interface{} {
		return rand.New(rand.NewSource(time.Now().UnixNano()))
	}}
	var conns []net.Conn
	for i := 0; i < nConns; i++ {
		conn, err := net.DialTimeout("udp", hostport, 1*time.Second)
		if err != nil {
			log.Fatalf("failed creating connection: %v", err)
		}
		conns = append(conns, conn)
	}
	return singleConnector{conns: conns, randPool: randPool}
}

func (s singleConnector) Conn(ctx context.Context, hash string) (net.Conn, error) {
	r := s.randPool.Get().(*rand.Rand)
	ind := r.Int() % len(s.conns)
	s.randPool.Put(r)
	return s.conns[ind], nil
}

func (singleConnector) Return(net.Conn, error) {
	return
}

/*
 * VENEUR REQUEST CODE
 */

type veneurLoadGenerator struct {
	f metricingester.ForwardingIngester
	m metricingester.Metric
}

func (v veneurLoadGenerator) run() {
	v.f.Ingest(context.Background(), v.m)
}
