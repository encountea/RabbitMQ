package main

import (
	"database/sql"
	"encoding/json"
	"fmt"

	// "net/http"

	// "github.com/gorilla/mux"
	_ "github.com/lib/pq"
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

// func ordersHandler(w http.ResponseWriter, r *http.Request) {

// }

func main() {
	// router := mux.NewRouter()
	// router.HandleFunc("/orders/{id:[0-9]+}", ordersHandler)
	// http.Handle("/", router)

	// fmt.Println("Server is listening...")
	// http.ListenAndServe(":8181", nil)

	// fmt.Println("Consumer Application")

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	ifError(err)

	defer conn.Close()

	ch, err := conn.Channel()
	ifError(err)
	defer ch.Close()

	msgs, err := ch.Consume(
		"TestQueue",
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		if err != nil {
			fmt.Println(err)
			panic(err)
		}
	}

	// connection string
	psqlconn := "host=localhost port=5432 user=postgres password=postgres dbname=orders sslmode=disable"

	// open database
	db, err := sql.Open("postgres", psqlconn)
	CheckError(err)

	// close database
	defer db.Close()

	// check db
	err = db.Ping()
	CheckError(err)

	fmt.Println("Connected!")

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			var order Order
			err := json.Unmarshal(d.Body, &order)
			if err != nil {
				panic(err)
			}
			_, err = db.Exec("insert into orders (order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)", order.Order_uid, order.Track_number, order.Entry, order.Locale, order.Internal_signature, order.Customer_id, order.Delivery_service, order.Shardkey, order.Sm_id, order.Date_created, order.Oof_shard)
			if err != nil {
				panic(err)
			}
			fmt.Printf("Recieved message: %s\n", d.Body)
			// insertStmt := `insert into orders values ('b563feb7b2b84b6test2', 'WBILMTESTTRACK', 'WBIL', 'en', ' ', 'test', 'meest', '9', 99, '2021-11-26T06:22:19Z', '1')`
			// _, e := db.Exec(insertStmt)
			// CheckError(e)

			// dynamic
			// insertDynStmt := `insert into "Students"("Name", "Roll") values($1, $2)`
			// _, e = db.Exec(insertDynStmt, "Jane", 2)
			// CheckError(e)
		}

	}()

	fmt.Println("Successfully connected to out RabbitMQ instance")
	fmt.Println(" [*] - waiting for messages")
	<-forever
}

// const (
// 	host     = "localhost"
// 	port     = 5432
// 	user     = "postgres"
// 	password = "postgres"
// 	dbname   = "orders"
// )

// func main() {
// 		 // connection string
// 	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

// 		 // open database
// 	db, err := sql.Open("postgres", psqlconn)
// 	CheckError(err)

// 		 // close database
// 	defer db.Close()

// 		 // check db
// 	err = db.Ping()
// 	CheckError(err)

// 	fmt.Println("Connected!")
// }

func CheckError(err error) {
	if err != nil {
		panic(err)
	}
}
