package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"

	gateclient "github.com/byteweap/meta/client/gate"
)

func main() {
	url := flag.String("url", "", "Gate WebSocket URL")
	flag.Parse()
	if *url == "" {
		log.Fatal("-url is required")
	}
	if err := enterGame(*url); err != nil {
		log.Fatal(err)
	}
}

func enterGame(url string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	header := http.Header{}
	if token := os.Getenv("GATE_DEMO_TOKEN"); token != "" {
		header.Set("Authorization", "Bearer "+token)
	}
	client, err := gateclient.Dial(ctx, url, header)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	enterGameRequest := protowire.AppendString(protowire.AppendTag(nil, 1, protowire.BytesType), "hello")
	response, err := client.Request(ctx, "game", 1, 1, enterGameRequest)
	if err != nil {
		return err
	}
	fmt.Println(protojson.Format(response))
	if failure := response.GetError(); failure != nil {
		return fmt.Errorf("enter game failed: %d %s", failure.GetCode(), failure.GetMessage())
	}
	return nil
}
