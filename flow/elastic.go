package flow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/BaritoLog/barito-flow/prome"

	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/olivere/elastic"
	log "github.com/sirupsen/logrus"
	pb "github.com/vwidjaya/barito-proto/producer"

	"github.com/zekroTJA/timedmap"
)

var counter int = 0

var (
	emailRegex       = regexp.MustCompile(`(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}`)
	phoneRegex       = regexp.MustCompile(`\b\d{3}[-.]?\d{3}[-.]?\d{4}\b`)
	addressRegex     = regexp.MustCompile(`(?i)(address|st|street|road|rd|avenue|ave|blvd|boulevard|lane|ln|dr|drive)\s+\d{1,5}\s+\w+(\s+\w+)*`)
	nameRegex        = regexp.MustCompile(`(?i)\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+\b`)
	dobRegex         = regexp.MustCompile(`\b\d{1,2}[-/]\d{1,2}[-/]\d{4}\b`)
	documentRegex    = regexp.MustCompile(`\b[0-9]{4}[ ]?[0-9]{4}[ ]?[0-9]{4}\b`)
	bankAccountRegex = regexp.MustCompile(`\b\d{9,18}\b`)
	creditScoreRegex = regexp.MustCompile(`(?i)\b(?:credit score|score|credit):?\s*\d{3}\b`)
	sexRegex         = regexp.MustCompile(`\b(?:Male|Female|Other)\b`)
)

const (
	DEFAULT_ELASTIC_DOCUMENT_TYPE = "_doc"
)

type Elastic interface {
	OnFailure(f func(*pb.Timber))
	Store(ctx context.Context, timber pb.Timber) (string, error)
	NewClient()
}

type elasticClient struct {
	client           *elastic.Client
	bulkProcessor    *elastic.BulkProcessor
	onFailureFunc    func(*pb.Timber)
	onStoreFunc      func(ctx context.Context, indexName, documentType, document string) (err error)
	jspbMarshaler    *jsonpb.Marshaler
	indexExistsCache *timedmap.TimedMap
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
		client:           c,
		bulkProcessor:    p,
		jspbMarshaler:    &jsonpb.Marshaler{},
		indexExistsCache: timedmap.New(1 * time.Minute),
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

func (e *elasticClient) Store(ctx context.Context, timber pb.Timber) (redactedDoc string, err error) {
	indexPrefix := timber.GetContext().GetEsIndexPrefix()
	indexName := fmt.Sprintf("%s-%s", indexPrefix, time.Now().Format("2006.01.02"))
	documentType := DEFAULT_ELASTIC_DOCUMENT_TYPE
	appSecret := timber.GetContext().GetAppSecret()

	indexCacheFound := e.indexExistsCache.GetValue(indexName)
	if indexCacheFound == nil {
		exists, _ := e.client.IndexExists(indexName).Do(ctx)
		if !exists {
			log.Warnf("ES index '%s' is not exist", indexName)
			_, err = e.client.CreateIndex(indexName).Do(ctx)
			instruESCreateIndex(err)
			if (err != nil) && (!strings.Contains(err.Error(), "resource_already_exists_exception")) {
				return
			}
		}
		e.indexExistsCache.Set(indexName, true, 1*time.Minute)
	}

	document, err := ConvertTimberToEsDocumentString(timber, e.jspbMarshaler)
	if err != nil {
		prome.IncreaseConsumerTimberConvertError(indexPrefix)
		return "", err
	}

	redactedDoc, err = redactJSONValuesStream(document)
	if err != nil {
		prome.IncreaseConsumerTimberConvertError(indexPrefix)
		return "", fmt.Errorf("error in redacting doc, err: %s", err.Error())
	}

	err = e.onStoreFunc(ctx, indexName, documentType, redactedDoc)
	counter++
	instruESStore(appSecret, err)

	return
}

func redactPIIData(jsonStr string) string {
	redacted := emailRegex.ReplaceAllString(jsonStr, "[REDACTED]")
	redacted = phoneRegex.ReplaceAllString(redacted, "[REDACTED]")
	redacted = addressRegex.ReplaceAllString(redacted, "[REDACTED]")
	redacted = nameRegex.ReplaceAllString(redacted, "[REDACTED]")
	redacted = dobRegex.ReplaceAllString(redacted, "[REDACTED]")
	redacted = documentRegex.ReplaceAllString(redacted, "[REDACTED]")
	redacted = bankAccountRegex.ReplaceAllString(redacted, "[REDACTED]")
	redacted = creditScoreRegex.ReplaceAllString(redacted, "[REDACTED]")
	redacted = sexRegex.ReplaceAllString(redacted, "[REDACTED]")
	return redacted
}

func redactJSONValuesStream(jsonStr string) (string, error) {
	var buffer bytes.Buffer
	decoder := json.NewDecoder(strings.NewReader(jsonStr))
	firstToken, err := decoder.Token()
	if err != nil {
		return "", err
	}
	if firstToken != json.Delim('{') {
		return "", fmt.Errorf("expected JSON object")
	}

	buffer.WriteString("{")
	isKey := true

	for decoder.More() {
		t, err := decoder.Token()
		if err != nil {
			return "", err
		}
		switch v := t.(type) {
		case string:
			if isKey {
				if !strings.HasPrefix(buffer.String(), "{") {
					buffer.WriteString(",")
				}
				buffer.WriteString(fmt.Sprintf("%q:", v))
				isKey = false
			} else {
				redactedValue := redactPIIData(v)
				buffer.WriteString(fmt.Sprintf("%q", redactedValue))
				buffer.WriteString(",")
				isKey = true
			}
		default:
			rawValue, err := json.Marshal(v)
			if err != nil {
				return "", err
			}
			buffer.Write(rawValue)
		}
	}

	buffer.Truncate(buffer.Len() - 1)
	buffer.WriteString("}")

	return buffer.String(), nil
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
