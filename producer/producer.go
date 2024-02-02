package main

import (
	"encoding/json"
	"fmt"

	"github.com/streadway/amqp"
)

type Order struct {
	Order_uid          string `json:"order_uid"`
	Track_number       string `json:"track_number"`
	Entry              string `json:"entry"`
	Locale             string `json:"locale"`
	Internal_signature string `json:"internal_signature"`
	Customer_id        string `json:"customer_id"`
	Delivery_service   string `json:"delivery_service"`
	Shardkey           string `json:"shardkey"`
	Sm_id              int    `json:"sm_id"`
	Date_created       string `json:"date_created"`
	Oof_shard          int    `json:"oof_shard"`
}

func ifError(err error) {
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
}

func main() {
	fmt.Println("Hello, Rabbit")
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	ifError(err)
	defer conn.Close()

	fmt.Println("Successfully connected to our RabbitMQ Instance")

	ch, err := conn.Channel()
	ifError(err)
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"TestQueue",
		false,
		false,
		false,
		false,
		nil,
	)

	ifError(err)

	fmt.Println(q)

	order := Order{
		Order_uid:          "b563fe",
		Track_number:       "WBILMTESTTRACK",
		Entry:              "WBIL",
		Locale:             "en",
		Internal_signature: "",
		Customer_id:        "test",
		Delivery_service:   "meest",
		Shardkey:           "9",
		Sm_id:              99,
		Date_created:       "2021-11-26T06:22:19Z",
		Oof_shard:          1,
	}

	jsonbytes, err := json.Marshal(&order)
	if err != nil {
		fmt.Println(err)
	}

	// deserialization := Order{}
	// err = json.Unmarshal(jsonbytes, &deserialization)

	if err != nil {
		fmt.Println(err)
	}

	err = ch.Publish(
		"",
		"TestQueue",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(jsonbytes),
		},
	)
	ifError(err)

	fmt.Println("Successfully published message to Queue")
}
