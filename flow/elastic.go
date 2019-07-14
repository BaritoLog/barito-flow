package flow

import (
	"context"
	"fmt"

	"time"

	"github.com/BaritoLog/barito-flow/es"
	"github.com/olivere/elastic"
	log "github.com/sirupsen/logrus"
)

var counter int = 0

type Elastic interface {
	OnFailure(f func(*Timber))
	Store(ctx context.Context, timber Timber)
	NewClient()
}

type elasticClient struct {
	client        *elastic.Client
	bulkProcessor *elastic.BulkProcessor
	onFailureFunc func(*Timber)
	onStoreFunc   func(indexName, documentType string, document map[string]interface{}) (err error)
}

type esConfig struct {
	indexMethod string
	bulkSize    int
	flushMs     time.Duration
	printTPS    bool
}

func NewEsConfig(indexMethod string, bulkSize int, flushMs time.Duration, printTPS bool) esConfig {

	return esConfig{
		indexMethod: indexMethod,
		bulkSize:    bulkSize,
		flushMs:     flushMs,
		printTPS:    printTPS,
	}
}

func NewElastic(retrierFunc *ElasticRetrier, esConfig esConfig, urls ...string) (client elasticClient, err error) {

	c, err := elastic.NewClient(
		elastic.SetURL(urls...),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
		elastic.SetRetrier(retrierFunc),
	)

	p, err := c.BulkProcessor().
		BulkActions(esConfig.bulkSize).
		FlushInterval(esConfig.flushMs * time.Millisecond).
		Do(context.Background())

	if esConfig.printTPS {
		printThroughputPerSecond()
	}

	client = elasticClient{
		client:        c,
		bulkProcessor: p,
	}

	if esConfig.indexMethod == "BulkProcessor" {
		client.onStoreFunc = client.bulkInsert
	} else if esConfig.indexMethod == "SingleInsert" {
		client.onStoreFunc = client.singleInsert
	}

	return
}

func printThroughputPerSecond() {
	t := time.NewTicker(1000 * time.Millisecond)

	go func() {
		for _ = range t.C {
			fmt.Println()
			fmt.Println("-------------------------------------")
			fmt.Println("PROCESSED:   ", counter)
			fmt.Println("-------------------------------------")
			fmt.Println()
			counter = 0
		}
	}()
}

func (e *elasticClient) Store(ctx context.Context, timber Timber) (err error) {
	indexPrefix := timber.Context().ESIndexPrefix
	documentType := timber.Context().ESDocumentType
	indexName := fmt.Sprintf("%s-%s", indexPrefix, time.Now().Format("2006.01.02"))
	appSecret := timber.Context().AppSecret

	exists, _ := e.client.IndexExists(indexName).Do(ctx)

	if !exists {
		log.Warnf("ES index '%s' is not exist", indexName)
		index := elasticCreateIndex(indexPrefix)
		_, err = e.client.CreateIndex(indexName).
			BodyJson(index).
			Do(ctx)
		instruESCreateIndex(err)
		if err != nil {
			return
		}
	}

	document := ConvertTimberToElasticDocument(timber)

	err = e.onStoreFunc(indexName, documentType, document)
	counter++
	instruESStore(appSecret, err)

	return
}

func (e *elasticClient) OnFailure(f func(*Timber)) {
	e.onFailureFunc = f
}

func (e *elasticClient) bulkInsert(indexName, documentType string, document map[string]interface{}) (err error) {
	r := elastic.NewBulkIndexRequest().
		Index(indexName).
		Type(documentType).
		Doc(document)
	e.bulkProcessor.Add(r)
	return
}

func (e *elasticClient) singleInsert(indexName, documentType string, document map[string]interface{}) (err error) {
	_, err = e.client.Index().
		Index(indexName).
		Type(documentType).
		BodyJson(document).
		Do(context.Background())
	return
}

func elasticCreateIndex(indexPrefix string) *es.Index {

	return &es.Index{
		Template: fmt.Sprintf("%s-*", indexPrefix),
		Version:  60001,
		Settings: map[string]interface{}{
			"index.refresh_interval": "5s",
		},
		Doc: es.NewMappings().
			AddDynamicTemplate("message_field", es.MatchConditions{
				PathMatch:        "@message",
				MatchMappingType: "string",
				Mapping: es.MatchMapping{
					Type:  "text",
					Norms: false,
				},
			}).
			AddDynamicTemplate("string_fields", es.MatchConditions{
				Match:            "*",
				MatchMappingType: "string",
				Mapping: es.MatchMapping{
					Type:  "text",
					Norms: false,
					Fields: map[string]es.Field{
						"keyword": es.Field{
							Type:        "keyword",
							IgnoreAbove: 256,
						},
					},
				},
			}).
			AddPropertyWithType("@timestamp", "date"),
	}
}
