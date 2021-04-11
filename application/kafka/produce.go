package kafka

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/NatanTavares/Delivery-Simulator/application/route"
	"github.com/NatanTavares/Delivery-Simulator/infra/kafka"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

func Produce(msg *ckafka.Message) {
	producer := kafka.NewKafkaProducer()
	route := route.NewRoute()

	json.Unmarshal(msg.Value, &route)

	route.LoadPositions()
	postions, err := route.ExportJsonPositions()
	if err != err {
		log.Println(err.Error())
	}

	for _, p := range postions {
		kafka.Publish(p, os.Getenv("KafkaProduceTopic"), producer)
		time.Sleep(time.Millisecond * 500)
	}
}
