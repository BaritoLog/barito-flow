package flow

import (
	"context"
	"fmt"

	"github.com/BaritoLog/barito-flow/prome"

	"time"

	"github.com/BaritoLog/barito-flow/es"
	"github.com/golang/protobuf/jsonpb"
	"github.com/olivere/elastic"
	log "github.com/sirupsen/logrus"
	pb "github.com/vwidjaya/barito-proto/producer"
)

var counter int = 0

type Elastic interface {
	OnFailure(f func(*pb.Timber))
	Store(ctx context.Context, timber pb.Timber) error
	NewClient()
}

type elasticClient struct {
	client        *elastic.Client
	bulkProcessor *elastic.BulkProcessor
	onFailureFunc func(*pb.Timber)
	onStoreFunc   func(ctx context.Context, indexName, documentType, document string) (err error)
	jspbMarshaler *jsonpb.Marshaler
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

func NewElastic(retrierFunc *ElasticRetrier, esConfig esConfig, urls []string, elasticUsername string, elasticPassword string) (client elasticClient, err error) {

	c, err := elastic.NewClient(
		elastic.SetURL(urls...),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
		elastic.SetRetrier(retrierFunc),
		elastic.SetBasicAuth(elasticUsername, elasticPassword),
	)

	beforeBulkFunc, afterBulkFunc := getCommitCallback()

	p, err := c.BulkProcessor().
		BulkActions(esConfig.bulkSize).
		FlushInterval(esConfig.flushMs * time.Millisecond).
		Before(beforeBulkFunc).
		After(afterBulkFunc).
		Do(context.Background())

	if esConfig.printTPS {
		printThroughputPerSecond()
	}

	client = elasticClient{
		client:        c,
		bulkProcessor: p,
		jspbMarshaler: &jsonpb.Marshaler{},
	}

	if esConfig.indexMethod == "BulkProcessor" {
		client.onStoreFunc = client.bulkInsert
	} else if esConfig.indexMethod == "SingleInsert" {
		client.onStoreFunc = client.singleInsert
	}

	return
}

func getCommitCallback() (func(int64, []elastic.BulkableRequest), func(int64, []elastic.BulkableRequest, *elastic.BulkResponse, error)) {
	var start time.Time
	beforeCommitCallback := func(executionId int64, requests []elastic.BulkableRequest) {
		start = time.Now()
	}
	afterCommitCallback := func(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
		diff := float64(time.Now().Sub(start).Nanoseconds()) / float64(1000000000)
		prome.ObserveBulkProcessTime(diff)

		for _, response := range response.Items {
			for _, responseItem := range response {
				errorReason := ""
				if responseItem.Error != nil {
					errorReason = responseItem.Error.Reason
				}
				prome.IncreaseLogStoredCounter(responseItem.Index, responseItem.Result, responseItem.Status, errorReason)
			}
		}
	}
	return beforeCommitCallback, afterCommitCallback
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

func (e *elasticClient) Store(ctx context.Context, timber pb.Timber) (err error) {
	indexPrefix := timber.GetContext().GetEsIndexPrefix()
	indexName := fmt.Sprintf("%s-%s", indexPrefix, time.Now().Format("2006.01.02"))
	documentType := timber.GetContext().GetEsDocumentType()
	appSecret := timber.GetContext().GetAppSecret()

	exists, _ := e.client.IndexExists(indexName).Do(ctx)

	if !exists {
		log.Warnf("ES index '%s' is not exist", indexName)
		index := elasticCreateIndex()
		_, err = e.client.CreateIndex(indexName).
			BodyJson(index).
			Do(ctx)
		instruESCreateIndex(err)
		if err != nil {
			return
		}
	}

	document := ConvertTimberToEsDocumentString(timber, e.jspbMarshaler)

	err = e.onStoreFunc(ctx, indexName, documentType, document)
	counter++
	instruESStore(appSecret, err)

	return
}

func (e *elasticClient) OnFailure(f func(*pb.Timber)) {
	e.onFailureFunc = f
}

func (e *elasticClient) bulkInsert(_ context.Context, indexName, documentType, document string) (err error) {
	r := elastic.NewBulkIndexRequest().
		Index(indexName).
		Type(documentType).
		Doc(document)
	e.bulkProcessor.Add(r)
	return
}

func (e *elasticClient) singleInsert(ctx context.Context, indexName, documentType, document string) (err error) {
	_, err = e.client.Index().
		Index(indexName).
		Type(documentType).
		BodyString(document).
		Do(ctx)
	return
}

func elasticCreateIndex() *es.Index {

	return &es.Index{
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
