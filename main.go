package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
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
