package main

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
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
	pub, err := sql.NewPublisher(`postgres://postgres:postgres@localhost:5438/dbname?sslmode=disable&select-path=select.sql&insert-path=insert.sql`, logger)
	if err != nil {
		panic(err)
	}
	sub := createSub(logger)
	router.AddHandler(
		"test_handler",
		"test_gg", // name of topic
		sub,
		"person", // name of Table
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



func createSub(logger watermill.LoggerAdapter) message.Subscriber {
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = "go-kafka-consumer"
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest

	kafkaSubscriber, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:     []string{"localhost:9092"},
			Unmarshaler: kafka.DefaultMarshaler{},
			OverwriteSaramaConfig: saramaConfig,
		},
		logger,
	)
	if err != nil {
		log.Fatalln(err)
	}

	return kafkaSubscriber
}