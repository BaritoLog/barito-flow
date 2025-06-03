package flow

import (
	"context"
	"fmt"
	"net/http"

	"github.com/BaritoLog/barito-flow/prome"

	"time"

	pb "github.com/bentol/barito-proto/producer"
	"github.com/golang/protobuf/jsonpb"
	"github.com/olivere/elastic/v7"
	log "github.com/sirupsen/logrus"

	"github.com/zekroTJA/timedmap"
)

var counter int = 0

const (
	DEFAULT_ELASTIC_DOCUMENT_TYPE = "_doc"

	IndexMethodBulkProcessor = "BulkProcessor"
	IndexMethodDatastream    = "DataStream"
	IndexMethodSingleInsert  = "SingleInsert"
)

type Elastic interface {
	OnFailure(f func(*pb.Timber))
	Store(ctx context.Context, timber pb.Timber) error
	NewClient()
}

type elasticClient struct {
	client                             *elastic.Client
	bulkProcessor                      *elastic.BulkProcessor
	onFailureFunc                      func(*pb.Timber)
	onStoreFunc                        func(ctx context.Context, indexName, documentType, document string) (err error)
	jspbMarshaler                      *jsonpb.Marshaler
	indexExistsCache                   *timedmap.TimedMap
	useDataStream                      bool
	dataStreamDefaultComponentTemplate string

	redactor Redactor
}

type Redactor interface {
	Redact(indexName, document string) (string, error)
}

type DummyRedactor struct{}

func (r *DummyRedactor) Redact(indexName, document string) (string, error) {
	return document, nil
}

type esConfig struct {
	indexMethod                        string
	bulkSize                           int
	flushMs                            time.Duration
	printTPS                           bool
	dataStreamDefaultComponentTemplate string
}

func NewEsConfig(indexMethod string, bulkSize int, flushMs time.Duration, printTPS bool, dataStreamDefaultComponentTemplate string) esConfig {

	return esConfig{
		indexMethod:                        indexMethod,
		bulkSize:                           bulkSize,
		flushMs:                            flushMs,
		printTPS:                           printTPS,
		dataStreamDefaultComponentTemplate: dataStreamDefaultComponentTemplate,
	}
}

func NewElastic(retrierFunc *ElasticRetrier, esConfig esConfig, urls []string, elasticUsername string, elasticPassword string, httpClient *http.Client) (client elasticClient, err error) {
	if httpClient == nil {
		httpClient = &http.Client{}
	}

	c, err := elastic.NewClient(
		elastic.SetURL(urls...),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
		elastic.SetRetrier(retrierFunc),
		elastic.SetBasicAuth(elasticUsername, elasticPassword),
		elastic.SetGzip(true),
		elastic.SetHttpClient(httpClient),
	)

	if err != nil {
		return
	}

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
		client:                             c,
		bulkProcessor:                      p,
		jspbMarshaler:                      &jsonpb.Marshaler{},
		indexExistsCache:                   timedmap.New(1 * time.Minute),
		redactor:                           &DummyRedactor{},
		useDataStream:                      false,
		dataStreamDefaultComponentTemplate: esConfig.dataStreamDefaultComponentTemplate,
	}

	if esConfig.indexMethod == IndexMethodBulkProcessor {
		client.onStoreFunc = client.bulkInsert
	} else if esConfig.indexMethod == IndexMethodDatastream {
		client.onStoreFunc = client.bulkInsertDataStream
		client.useDataStream = true
	} else if esConfig.indexMethod == IndexMethodSingleInsert {
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
		for range t.C {
			fmt.Println()
			fmt.Println("-------------------------------------")
			fmt.Println("PROCESSED:   ", counter)
			fmt.Println("-------------------------------------")
			fmt.Println()
			counter = 0
		}
	}()
}

func (e *elasticClient) WithRedactor(r Redactor) *elasticClient {
	e.redactor = r
	return e
}

func (e *elasticClient) ensureIndexIsExistsRegularIndex(ctx context.Context, indexName string) bool {
	log.Warnf("ES index '%s' is not exist", indexName)
	_, err := e.client.CreateIndex(indexName).Do(ctx)
	instruESCreateIndex(err)
	if err != nil {
		log.Errorf("Error creating index: %s", err)
		return false
	}
	e.indexExistsCache.Set(indexName, true, 1*time.Minute)
	return true
}

func (e *elasticClient) ensureIndexIsExistsDataStream(ctx context.Context, datastreamName string) bool {
	log.Warnf("ES datastream index '%s' is not exist", datastreamName)
	payload := fmt.Sprintf(
		`{"index_patterns":["%s"],"data_stream":{},"composed_of":["%s"],"priority":200,"_meta":{"description":"default template"}} `,
		datastreamName,
		e.dataStreamDefaultComponentTemplate,
	)
	_, err := e.client.IndexPutIndexTemplate(datastreamName).
		Name(datastreamName + "-template").
		BodyString(payload).
		Do(ctx)

	if err != nil {
		log.Errorf("Error creating index template %s: %s", datastreamName, err)
		return false
	}

	return true
}

func (e *elasticClient) ensureIndexIsExists(ctx context.Context, indexName string) bool {
	indexCacheFound := e.indexExistsCache.GetValue(indexName)
	if indexCacheFound != nil {
		return true
	}

	exists, err := e.client.IndexExists(indexName).Do(ctx)
	if err != nil {
		log.Errorf("Error checking if index exists: %s", err)
		return false
	}

	if !exists {
		if e.useDataStream {
			return e.ensureIndexIsExistsDataStream(ctx, indexName)
		}
		return e.ensureIndexIsExistsRegularIndex(ctx, indexName)
	}
	return true
}

func (e *elasticClient) Store(ctx context.Context, timber pb.Timber) (err error) {
	indexPrefix := timber.GetContext().GetEsIndexPrefix()
	indexName := fmt.Sprintf("%s-%s", indexPrefix, time.Now().Format("2006.01.02"))
	if e.useDataStream {
		indexName = indexPrefix
	}
	documentType := DEFAULT_ELASTIC_DOCUMENT_TYPE
	appSecret := timber.GetContext().GetAppSecret()

	// ensure index is exists before push the logs
	for {
		if e.ensureIndexIsExists(ctx, indexName) {
			break
		}
		time.Sleep(10 * time.Second)
	}

	document, err := ConvertTimberToEsDocumentString(timber, e.jspbMarshaler)
	if err != nil {
		prome.IncreaseConsumerTimberConvertError(indexPrefix)
		return
	}

	redactDocument, err := e.redactor.Redact(indexPrefix, document)
	if err != nil {
		log.Error("Error redacting document: ", err)
		return
	}

	err = e.onStoreFunc(ctx, indexName, documentType, redactDocument)
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

func (e *elasticClient) bulkInsertDataStream(_ context.Context, indexName, documentType, document string) (err error) {
	r := elastic.NewBulkIndexRequest().
		OpType("create").
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
