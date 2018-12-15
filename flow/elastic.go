package flow

import (
	"context"
	"fmt"

	"time"

	"github.com/BaritoLog/barito-flow/es"
	"github.com/olivere/elastic"
	log "github.com/sirupsen/logrus"
)

type Elastic interface {
	OnFailure(f func(*Timber))
	Store(ctx context.Context, timber Timber)
	NewClient()
}

type elasticClient struct {
	client        *elastic.Client
	onFailureFunc func(*Timber)
}

func NewElastic(retrierFunc *ElasticRetrier, urls ...string) (client elasticClient, err error) {

	c, err := elastic.NewClient(
		elastic.SetURL(urls...),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
		elastic.SetRetrier(retrierFunc),
	)

	return elasticClient{
		client: c,
	}, err
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

	_, err = e.client.Index().
		Index(indexName).
		Type(documentType).
		BodyJson(document).
		Do(ctx)
	instruESStore(appSecret, err)

	return
}

func (e *elasticClient) OnFailure(f func(*Timber)) {
	e.onFailureFunc = f
}

func elasticCreateIndex(indexPrefix string) *es.Index {

	return &es.Index{
		Template: fmt.Sprintf("%s-*", indexPrefix),
		Version:  60001,
		Settings: map[string]interface{}{
			"index.refresh_interval": "5s",
			// "index.read_only_allow_delete": "false",
		},
		Doc: es.NewMappings().
			// TODO: revisit this. @message not longer required
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
							Type:        "text",
							IgnoreAbove: 256,
						},
					},
				},
			}).
			AddPropertyWithType("@timestamp", "date"),
	}
}
