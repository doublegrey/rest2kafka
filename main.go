package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
)

type Request struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": os.Getenv("BROKER")})
	if err != nil {
		log.Fatal("Failed to create producer:", err)
	}
	r := gin.Default()

	r.POST("/:topic", func(c *gin.Context) {
		if c.GetHeader("Authorization") != os.Getenv("TOKEN") {
			c.Status(http.StatusUnauthorized)
			return
		}
		var request Request
		err = c.ShouldBindJSON(&request)
		if err != nil {
			log.Printf("Failed to unmarshal request body: %v", err)
			c.Status(http.StatusBadRequest)
			return
		}
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
	})
	r.Run(fmt.Sprintf(":%s", os.Getenv("PORT")))
}
