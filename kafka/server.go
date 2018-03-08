package kafka

type Kafka interface {
	Producer() (producer Producer, err error)
}

type Server struct {
	brokers string
}

func NewKafka(brokers string) Kafka {
	return &Server{
		brokers: brokers,
	}
}
