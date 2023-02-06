package main

import (
	"context"
	"ethparser/crawler"
	"ethparser/graph"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	// _ "github.com/99designs/gqlgen"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"
)

const defaultPort = "8080"

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Please provide your TOKEN to get connected")
		os.Exit(-1)
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	fetcher := &crawler.Fetcher{
		NetworkURI: fmt.Sprintf("wss://mainnet.infura.io/ws/v3/%s", os.Args[1]),
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		interruptChannel := make(chan os.Signal, 1)
		signal.Notify(interruptChannel, os.Interrupt, syscall.SIGTERM)
		<-interruptChannel
		fmt.Println("Received interrupt, exiting...")
		cancel()
	}()
	go fetcher.Start(ctx)

	handler := handler.NewDefaultServer(graph.NewExecutableSchema(graph.Config{
		Resolvers: &graph.Resolver{
			BlockFetcher: fetcher,
		},
	}))

	router := http.NewServeMux()
	router.Handle("/", playground.Handler("GraphQL playground", "/query"))
	router.Handle("/query", handler)

	log.Printf("connect to http://localhost:%s/ for GraphQL playground", port)
	srv := http.Server{
		Handler:           router,
		Addr:              fmt.Sprint(":" + port),
		ReadHeaderTimeout: 0,
	}
	go func() {
		<-ctx.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			fmt.Println("Failed to Shutdown server, error=", err)
		}
	}()
	log.Fatal(srv.ListenAndServe())
}
