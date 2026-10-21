package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	sdk "github.com/mnemosyne-ai-systems/chorus-sdk-go"
	"github.com/mnemosyne-ai-systems/chorus-sdk-go/grpctransport"
)

func main() {
	endpoint := flag.String("endpoint", "127.0.0.1:9000", "Chorus node endpoint")
	sid := flag.String("sid", "example-session", "session ID")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	dialer := grpctransport.NewDialer()
	defer dialer.Close()

	routes, err := dialer.RouteSource(*endpoint)
	if err != nil {
		log.Fatal(err)
	}

	client := sdk.NewClient(routes, dialer)
	loadCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	if err := client.Reload(loadCtx); err != nil {
		cancel()
		log.Fatal(err)
	}
	cancel()

	client.StartRoutePoll(ctx)

	resp, err := client.PutSessionState(ctx, *sid, "example-key", []byte("hello"), 0)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("stored example-key at version %d", resp.Version)
}
