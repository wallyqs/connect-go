package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"context"
	"net/http"

	"github.com/HdrHistogram/hdrhistogram-go"
	hw "github.com/tylertreat/hdrhistogram-writer"
	"github.com/bufbuild/connect-go"
	pingv1 "github.com/bufbuild/connect-go/internal/gen/connect/ping/v1"
	"github.com/bufbuild/connect-go/internal/gen/connect/ping/v1/pingv1connect"
	"github.com/nats-io/nats.go"
	nrt "github.com/ripienaar/nats-roundtripper"
)

// Test Parameters
var (
	ServerA       string
	ServerB       string
	TargetPubRate int
	MsgSize       int
	NumPubs       int
	TestDuration  time.Duration
	HistFile      string
	Secure        bool
	TLSca         string
	TLSkey        string
	TLScert       string
	UserCreds     string
)

var usageStr = `
Usage: latency-tests [options]
Test Options:
    -sa <url>        ServerA (Publish) (default: nats://localhost:4222)
    -sb <url>        ServerB (Subscribe) (default: nats://localhost:4222)
    -sz <int>        Message size in bytes (default: 8)
    -tr <int>        Rate in msgs/sec (default: 1000)
    -tt <string>     Test duration (default: 5s)
    -hist <file>     Histogram output file
    -secure          Enable TLS without verfication (default: false)
    -tls_ca <string> TLS Certificate CA file
    -tls_key <file>  TLS Private Key
    -tls_cert <file> TLS Certificate
    -creds <file>    User Credentials
`

func usage() {
	log.Fatalf(usageStr + "\n")
}

// waitForRoute tests a subscription in the server to ensure subject interest
// has been propagated between servers.  Otherwise, we may miss early messages
// when testing with clustered servers and the test will hang.
func waitForRoute(pnc, snc *nats.Conn) {

	// No need to continue if using one server
	if strings.Compare(pnc.ConnectedServerId(), snc.ConnectedServerId()) == 0 {
		return
	}

	// Setup a test subscription to let us know when a message has been received.
	// Use a new inbox subject as to not skew results
	var routed int32
	subject := nats.NewInbox()
	sub, err := snc.Subscribe(subject, func(msg *nats.Msg) {
		atomic.AddInt32(&routed, 1)
	})
	if err != nil {
		log.Fatalf("Couldn't subscribe to test subject %s: %v", subject, err)
	}
	defer sub.Unsubscribe()
	snc.Flush()

	// Periodically send messages until the test subscription receives
	// a message.  Allow for two seconds.
	start := time.Now()
	for atomic.LoadInt32(&routed) == 0 {
		if time.Since(start) > (time.Second * 2) {
			log.Fatalf("Couldn't receive end-to-end test message.")
		}
		if err = pnc.Publish(subject, nil); err != nil {
			log.Fatalf("Couldn't publish to test subject %s:  %v", subject, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

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

func main() {
	start := time.Now()

	flag.StringVar(&ServerA, "sa", nats.DefaultURL, "ServerA - Publisher")
	flag.StringVar(&ServerB, "sb", nats.DefaultURL, "ServerB - Subscriber")
	flag.IntVar(&TargetPubRate, "tr", 1000, "Target Publish Rate")
	flag.IntVar(&MsgSize, "sz", 8, "Message Payload Size")
	flag.DurationVar(&TestDuration, "tt", 5*time.Second, "Target Test Time")
	flag.StringVar(&HistFile, "hist", "", "Histogram and Raw Output")
	flag.BoolVar(&Secure, "secure", false, "Use a TLS Connection w/o verification")
	flag.StringVar(&TLSkey, "tls_key", "", "Private key file")
	flag.StringVar(&TLScert, "tls_cert", "", "Certificate file")
	flag.StringVar(&TLSca, "tls_ca", "", "Certificate CA file")
	flag.StringVar(&UserCreds, "creds", "", "User Credentials File")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	// Slow down GC.
	debug.SetGCPercent(500)

	NumPubs = int(TestDuration/time.Second) * TargetPubRate

	if MsgSize < 8 {
		log.Fatalf("Message Payload Size must be at least %d bytes\n", 8)
	}

	// Setup connection options
	var opts []nats.Option
	if Secure {
		opts = append(opts, nats.Secure())
	}
	if TLSca != "" {
		opts = append(opts, nats.RootCAs(TLSca))
	}
	if TLScert != "" {
		opts = append(opts, nats.ClientCert(TLScert, TLSkey))
	}
	if UserCreds != "" {
		opts = append(opts, nats.UserCredentials(UserCreds))
	}

	c1, err := nats.Connect(ServerA, opts...)
	if err != nil {
		log.Fatalf("Could not connect to ServerA: %v", err)
	}
	defer c1.Close()

	// gRPC Publisher --------------------------------------------

	// Swap to use NATS transport.
	t, _ := nrt.New(nrt.WithConnection(c1))
	httpClient1 := &http.Client{Transport: t}

	// Make the http protocol flow through NATS instead.
	client := pingv1connect.NewPingServiceClient(
		httpClient1,
		"http://localhost.nats",
	)
	// ----------------------------------------------------------

	c2, err := nats.Connect(ServerB, opts...)
	if err != nil {
		log.Fatalf("Could not connect to ServerB: %v", err)
	}
	defer c2.Close()

	// gRPC Service -----------------------------------------------
	mux := http.NewServeMux()
	mux.Handle(
		pingv1connect.NewPingServiceHandler(
			&ExamplePingServer{},
		),
	)

	// Connect the 'connect-go' server to a NATS Server swapping
	// to work over a NATS transport.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nts, err := nrt.New(nrt.WithConnection(c2))
	if err != nil {
		log.Fatal(err)
	}
	go nts.ListenAndServ(ctx, "localhost.nats", mux)

	// Do some quick RTT calculations
	log.Println("==============================")
	now := time.Now()
	c1.Flush()
	log.Printf("Pub Server RTT : %v\n", fmtDur(time.Since(now)))

	now = time.Now()
	c2.Flush()
	log.Printf("Sub Server RTT : %v\n", fmtDur(time.Since(now)))

	// Duration tracking
	durations := make([]time.Duration, 0, NumPubs)

	// Wait for all messages to be received.
	var wg sync.WaitGroup
	wg.Add(1)

	// Make sure interest is set for subscribe before publish since a different connection.
	c2.Flush()

	// wait for routes to be established so we get every message
	waitForRoute(c1, c2)

	log.Printf("Message Payload: %v\n", byteSize(MsgSize))
	log.Printf("Target Duration: %v\n", TestDuration)
	log.Printf("Target Msgs/Sec: %v\n", TargetPubRate)
	log.Printf("Target Band/Sec: %v\n", bps(TargetPubRate*MsgSize*2))
	log.Println("==============================")

	// Random payload
	// data := make([]byte, MsgSize)
	// io.ReadFull(rand.Reader, data)

	// For publish throttling
	delay := time.Second / time.Duration(TargetPubRate)
	pubStart := time.Now()

	// Throttle logic, crude I know, but works better then time.Ticker or
	// golang.org/x/time/rate.

	adjustAndSleep := func(now time.Time, count int) {
		r := rps(count, now.Sub(pubStart))
		adj := delay/20 + time.Nanosecond // 5%
		if r < TargetPubRate {
			delay -= adj
			if delay < 0 {
				delay = 0
			}
		} else if r > TargetPubRate {
			delay += adj
		}
		if delay > 0 {
			time.Sleep(delay)
		}
	}

	// Now publish
	for i := 0; i < NumPubs; i++ {
		now := time.Now()
		// Place the send time in the front of the payload.
		resp, err := client.Ping(
			ctx,
			connect.NewRequest(&pingv1.PingRequest{Number: int64(now.UnixNano())}),
		)
		sendTime := resp.Msg.Number
		durations = append(durations, time.Duration(time.Now().UnixNano()-sendTime))
		if err != nil {
			log.Fatal(err)
		}
		adjustAndSleep(now, i+1)
	}
	pubDur := time.Since(pubStart)
	subDur := time.Since(pubStart)

	// If we are writing to files, save the original unsorted data
	if HistFile != "" {
		if err := writeRawFile(HistFile+".raw", durations); err != nil {
			log.Printf("Unable to write raw output file: %v", err)
		}
	}

	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

	h := hdrhistogram.New(1, int64(durations[len(durations)-1]), 5)
	for _, d := range durations {
		h.RecordValue(int64(d))
	}

	log.Printf("HDR Percentiles:\n")
	log.Printf("10:       %v\n", fmtDur(time.Duration(h.ValueAtQuantile(10))))
	log.Printf("50:       %v\n", fmtDur(time.Duration(h.ValueAtQuantile(50))))
	log.Printf("75:       %v\n", fmtDur(time.Duration(h.ValueAtQuantile(75))))
	log.Printf("90:       %v\n", fmtDur(time.Duration(h.ValueAtQuantile(90))))
	log.Printf("99:       %v\n", fmtDur(time.Duration(h.ValueAtQuantile(99))))
	log.Printf("99.9:     %v\n", fmtDur(time.Duration(h.ValueAtQuantile(99.9))))
	log.Printf("99.99:    %v\n", fmtDur(time.Duration(h.ValueAtQuantile(99.99))))
	log.Printf("99.999:   %v\n", fmtDur(time.Duration(h.ValueAtQuantile(99.999))))
	log.Printf("99.9999:  %v\n", fmtDur(time.Duration(h.ValueAtQuantile(99.9999))))
	log.Printf("99.99999: %v\n", fmtDur(time.Duration(h.ValueAtQuantile(99.99999))))
	log.Printf("100:      %v\n", fmtDur(time.Duration(h.ValueAtQuantile(100.0))))
	log.Println("==============================")

	if HistFile != "" {
		pctls := hw.Percentiles{10, 25, 50, 75, 90, 99, 99.9, 99.99, 99.999, 99.9999, 99.99999, 100.0}
		hw.WriteDistributionFile(h, pctls, 1.0/1000000.0, HistFile+".histogram")
	}

	// Print results
	log.Printf("Actual Msgs/Sec: %d\n", rps(NumPubs, pubDur))
	log.Printf("Actual Band/Sec: %v\n", bps(rps(NumPubs, pubDur)*MsgSize*2))
	log.Printf("Minimum Latency: %v", fmtDur(durations[0]))
	log.Printf("Median Latency : %v", fmtDur(getMedian(durations)))
	log.Printf("Maximum Latency: %v", fmtDur(durations[len(durations)-1]))
	log.Printf("1st Sent Wall Time : %v", fmtDur(pubStart.Sub(start)))
	log.Printf("Last Sent Wall Time: %v", fmtDur(pubDur))
	log.Printf("Last Recv Wall Time: %v", fmtDur(subDur))
}

const fsecs = float64(time.Second)

func rps(count int, elapsed time.Duration) int {
	return int(float64(count) / (float64(elapsed) / fsecs))
}

// Just pretty print the byte sizes.
func byteSize(n int) string {
	sizes := []string{"B", "K", "M", "G", "T"}
	base := float64(1024)
	if n < 10 {
		return fmt.Sprintf("%d%s", n, sizes[0])
	}
	e := math.Floor(logn(float64(n), base))
	suffix := sizes[int(e)]
	val := math.Floor(float64(n)/math.Pow(base, e)*10+0.5) / 10
	f := "%.0f%s"
	if val < 10 {
		f = "%.1f%s"
	}
	return fmt.Sprintf(f, val, suffix)
}

func bps(n int) string {
	sizes := []string{"Bps", "Kbps", "Mbps", "Gbps", "Tbps"}
	base := float64(1024)
	n *= 8
	if n < 10 {
		return fmt.Sprintf("%d%s", n, sizes[0])
	}
	e := math.Floor(logn(float64(n), base))
	suffix := sizes[int(e)]
	val := math.Floor(float64(n)/math.Pow(base, e)*10+0.5) / 10
	f := "%.0f%s"
	if val < 10 {
		f = "%.1f%s"
	}
	return fmt.Sprintf(f, val, suffix)
}

func logn(n, b float64) float64 {
	return math.Log(n) / math.Log(b)
}

// Make time durations a bit prettier.
func fmtDur(t time.Duration) time.Duration {
	// e.g 234us, 4.567ms, 1.234567s
	return t.Truncate(time.Microsecond)
}

func getMedian(values []time.Duration) time.Duration {
	l := len(values)
	if l == 0 {
		log.Fatalf("empty set")
	}
	if l%2 == 0 {
		return (values[l/2-1] + values[l/2]) / 2
	}
	return values[l/2]
}

// writeRawFile creates a file with a list of recorded latency
// measurements, one per line.
func writeRawFile(filePath string, values []time.Duration) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, value := range values {
		fmt.Fprintf(f, "%f\n", float64(value.Nanoseconds())/1000000.0)
	}
	return nil
}
