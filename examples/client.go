// Copyright 2021-2022 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/bufbuild/connect-go"
	pingv1 "github.com/bufbuild/connect-go/internal/gen/connect/ping/v1"
	"github.com/bufbuild/connect-go/internal/gen/connect/ping/v1/pingv1connect"
	nrt "github.com/ripienaar/nats-roundtripper"
)

func main() {
	logger := log.New(os.Stdout, "", 0)

	// Swap to use NATS transport.
	t, _ := nrt.New(nrt.WithNatsServer("nats://localhost"))
	httpClient := &http.Client{Transport: t}

	// Make the http protocol flow through NATS instead.
	client := pingv1connect.NewPingServiceClient(
		httpClient,
		"http://localhost.nats",
	)
	response, err := client.Ping(
		context.Background(),
		connect.NewRequest(&pingv1.PingRequest{Number: 42}),
	)
	if err != nil {
		logger.Println("error:", err)
		return
	}
	logger.Println("response content-type:", response.Header().Get("Content-Type"))
	logger.Println("response message:", response.Msg)
}
