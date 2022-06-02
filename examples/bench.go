// Copyright 2022 The NATS Authors

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bufbuild/connect-go"
	pingv1 "github.com/bufbuild/connect-go/internal/gen/connect/ping/v1"
	"github.com/bufbuild/connect-go/internal/gen/connect/ping/v1/pingv1connect"
	"github.com/nats-io/nats.go"
	nrt "github.com/ripienaar/nats-roundtripper"
)

// Some sane defaults
const (
	DefaultNumMsgs = 100000
	DefaultNumPubs = 1
	DefaultNumSubs = 0
	HashModulo     = 1000
)

// ExamplePingServer implements some trivial business logic. The Protobuf
// definition for this API is in proto/connect/ping/v1/ping.proto.
type ExamplePingServer struct {
	pingv1connect.UnimplementedPingServiceHandler
}

// Ping implements pingv1connect.PingServiceHandler.
func (*ExamplePingServer) Ping(
	_ context.Context,
	request *connect.Request[pingv1.PingRequest],
) (*connect.Response[pingv1.PingResponse], error) {
	return connect.NewResponse(
		&pingv1.PingResponse{
			Number: request.Msg.Number,
			Text:   request.Msg.Text,
		},
	), nil
}

func usage() {
	log.Fatalf("Usage: nats-bench [-s server (%s)] [--tls] [-np NUM_PUBLISHERS] [-ns NUM_SUBSCRIBERS] [-n NUM_MSGS] \n", nats.DefaultURL)
}

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The NATS server URLs (separated by comma)")
	var tls = flag.Bool("tls", false, "Use TLS Secure Connection")
	var numPubs = flag.Int("np", DefaultNumPubs, "Number of Concurrent Clients")
	var numSubs = flag.Int("ns", DefaultNumSubs, "Number of Concurrent Services")
	var numMsgs = flag.Int("n", DefaultNumMsgs, "Number of Requests to Publish")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	// Setup the option block
	opts := nats.DefaultOptions
	opts.Servers = strings.Split(*urls, ",")
	for i, s := range opts.Servers {
		opts.Servers[i] = strings.Trim(s, " ")
	}
	opts.Secure = *tls

	var startwg sync.WaitGroup
	var donewg sync.WaitGroup

	donewg.Add(*numPubs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run Subscribers first
	startwg.Add(*numSubs)
	for i := 0; i < *numSubs; i++ {
		go runSubscriber(ctx, &startwg, &donewg, opts, (*numMsgs)*(*numPubs))
	}
	startwg.Wait()

	// Now Publishers
	startwg.Add(*numPubs)
	for i := 0; i < *numPubs; i++ {
		go runPublisher(ctx, &startwg, &donewg, opts, *numMsgs)
	}

	fmt.Printf("Starting benchmark\n")
	fmt.Printf("msgs=%d, pubs=%d, subs=%d\n", *numMsgs, *numPubs, *numSubs)

	startwg.Wait()
	start := time.Now()

	donewg.Wait()
	duration := time.Since(start)
	delta := duration.Seconds()
	total := float64((*numMsgs) * (*numPubs))
	fmt.Printf("\nNATS throughput is %s msgs/sec (duration: %.3fs)\n", commaFormat(int64(total/delta)), duration.Seconds())
}

func runPublisher(ctx context.Context, startwg, donewg *sync.WaitGroup, opts nats.Options, numMsgs int) {
	nc, err := opts.Connect()
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}
	defer nc.Close()

	// Swap to use NATS transport.
	t, _ := nrt.New(nrt.WithConnection(nc))
	httpClient := &http.Client{Transport: t}

	// Make the http protocol flow through NATS instead.
	client := pingv1connect.NewPingServiceClient(
		httpClient,
		"http://localhost.nats",
	)
	startwg.Done()

	for i := 0; i < numMsgs; i++ {
		_, err := client.Ping(
			ctx,
			connect.NewRequest(&pingv1.PingRequest{Number: 22}),
		)
		if err != nil {
			log.Fatal(err)
		}
		if i%HashModulo == 0 {
			fmt.Fprintf(os.Stderr, "#")
		}
	}

	donewg.Done()
}

func runSubscriber(ctx context.Context, startwg, donewg *sync.WaitGroup, opts nats.Options, numMsgs int) {
	nc, err := opts.Connect()
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}

	mux := http.NewServeMux()
	mux.Handle(
		pingv1connect.NewPingServiceHandler(
			&ExamplePingServer{},
		),
	)

	// Connect the 'connect-go' server to a NATS Server swapping
	// to work over a NATS transport.
	nts, err := nrt.New(nrt.WithConnection(nc))
	if err != nil {
		log.Fatal(err)
	}
	go nts.ListenAndServ(ctx, "localhost.nats", mux)

	startwg.Done()
}

func commaFormat(n int64) string {
	in := strconv.FormatInt(n, 10)
	out := make([]byte, len(in)+(len(in)-2+int(in[0]/'0'))/3)
	if in[0] == '-' {
		in, out[0] = in[1:], '-'
	}
	for i, j, k := len(in)-1, len(out)-1, 0; ; i, j = i-1, j-1 {
		out[j] = in[i]
		if i == 0 {
			return string(out)
		}
		if k++; k == 3 {
			j, k = j-1, 0
			out[j] = ','
		}
	}
}
