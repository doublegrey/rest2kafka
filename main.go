package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/linkedin/goavro"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Request struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

type TopicSchema struct {
	Created time.Time `json:"created"`
	Name    string    `json:"name"`
	Topic   string    `json:"topic"`
	Data    string    `json:"data"`
}
type TopicMessage struct {
	Offset    int64     `json:"offset"`
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

var upGrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": os.Getenv("BROKER")})
	if err != nil {
		log.Fatal("Failed to create producer:", err)
	}
	r := gin.Default()

	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://admin:welcome%4031415@20.52.39.84:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false"))
	if err != nil {
		log.Fatal(err)
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	r.GET("/:topic", func(c *gin.Context) {
		ws, _ := upGrader.Upgrade(c.Writer, c.Request, nil)
		defer ws.Close()
		mt, token, _ := ws.ReadMessage()

		if strings.ToLower(os.Getenv("AUTH")) == "true" {
			count, _ := client.Database("orch").Collection("users").CountDocuments(context.TODO(), bson.M{"token": string(token)})
			if count == 0 {
				c.Status(http.StatusUnauthorized)
				return
			}
		}
		ctx, cancel := context.WithCancel(context.TODO())

		run := true
		go func() {
			for run {
				_, b, err := ws.ReadMessage()
				if err != nil {
					run = false
					cancel()
				}
				if string(b) == "stop" {
					run = false
					cancel()
				}
			}
		}()

		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":               os.Getenv("BROKER"),
			"group.id":                        "rest2kafka_" + strconv.Itoa(rand.Intn(10000)),
			"session.timeout.ms":              6000,
			"go.events.channel.enable":        true,
			"enable.auto.commit":              false,
			"go.application.rebalance.enable": true,
			"enable.partition.eof":            true,
			"auto.offset.reset":               "earliest"})
		if err != nil {
			fmt.Println(err)
		}
		consumer.SubscribeTopics([]string{c.Param("topic")}, nil)
		for run {
			select {
			case <-ctx.Done():
				run = false
			case ev := <-consumer.Events():
				switch e := ev.(type) {
				case kafka.AssignedPartitions:
					consumer.Assign(e.Partitions)
				case kafka.RevokedPartitions:
					consumer.Unassign()
				case *kafka.Message:

					b, _ := json.Marshal(TopicMessage{Offset: int64(e.TopicPartition.Offset), Key: string(e.Key), Value: string(e.Value), Timestamp: e.Timestamp})
					ws.WriteMessage(mt, b)
				}
			}
		}
	})

	r.POST("/:topic", func(c *gin.Context) {
		var schemas []TopicSchema
		cursor, _ := client.Database("orch").Collection("schemas").Find(context.TODO(), bson.M{"topic": c.Param("topic")})
		if strings.ToLower(os.Getenv("AUTH")) == "true" {
			count, _ := client.Database("orch").Collection("users").CountDocuments(context.TODO(), bson.M{"token": c.GetHeader("Authorization")})
			if count == 0 {
				c.Status(http.StatusUnauthorized)
				return
			}
		}

		err = cursor.All(context.TODO(), &schemas)
		var latest TopicSchema
		for _, schema := range schemas {
			if schema.Created.After(latest.Created) {
				latest = schema
			}
		}
		valid := false

		var request Request
		err = c.ShouldBindJSON(&request)
		if err != nil {
			log.Printf("Failed to unmarshal request body: %v", err)
			c.Status(http.StatusBadRequest)
			return
		}

		bytes, _ := json.Marshal(request.Value)

		if len(latest.Data) > 0 {
			codec, _ := goavro.NewCodec(latest.Data)

			_, _, err = codec.NativeFromTextual(bytes)
			if err == nil {
				valid = true
			}
		} else {
			valid = true
		}

		if valid {
			topic := c.Param("topic")
			deliveryChan := make(chan kafka.Event)
			value, err := json.Marshal(request.Value)
			if err != nil {
				log.Printf("Failed to marshal value: %v", err)
				c.Status(http.StatusBadRequest)
				return
			}
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          value,
				Key:            []byte(request.Key),
			}, deliveryChan)
			e := <-deliveryChan
			m := e.(*kafka.Message)

			if err != nil || m.TopicPartition.Error != nil {
				log.Printf("Failed to produce message: %v\n", m.TopicPartition.Error)
			}
			close(deliveryChan)
			c.JSON(200, gin.H{"Partition": m.TopicPartition.Partition, "Offset": m.TopicPartition.Offset})
			return
		}
		c.Status(http.StatusBadRequest)
	})
	r.Run(fmt.Sprintf(":%s", os.Getenv("PORT")))
}
