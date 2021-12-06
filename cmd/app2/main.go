package main

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
	_ "github.com/lib/pq"
	"log"
	"postgres-test/sql"
)

func main() {
	logger := watermill.NewStdLogger(
		true, // debug
		true, // trace
	)
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatalln(err)
	}
	router.AddPlugin(plugin.SignalsHandler)
	router.AddMiddleware(middleware.Recoverer)
	sub, err := sql.NewSubscriber(`postgres://postgres:postgres@localhost:5438/dbname?sslmode=disable&select-path=select.sql`, logger)
	if err != nil {
		panic(err)
	}
	pub := createPub(logger)
	router.AddHandler(
		"test_handler",
		"test", // name of topic
		sub,
		"test_gg", // name of Table
		pub,
		func(msg *message.Message) ([]*message.Message, error) {
			fmt.Println("msg ==>", msg)
			return []*message.Message{msg}, nil
		},
	)

	if err = router.Run(context.Background()); err != nil {
		log.Panicln(err)
	}
}

func createPub(logger watermill.LoggerAdapter) message.Publisher {
	publisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:   []string{"localhost:9092"},
			Marshaler: kafka.DefaultMarshaler{},
		},
		logger,
	)
	if err != nil {
		panic(err)
	}


	return publisher
}