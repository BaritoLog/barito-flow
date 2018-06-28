package flow

import (
	"context"
	"fmt"

	"time"

	"github.com/BaritoLog/barito-flow/es"
	"github.com/BaritoLog/instru"
	"github.com/olivere/elastic"
)

const (
	MESSAGE_TYPE = "fluentd"
	INDEX_PREFIX = "baritolog"
)

type ElasticStoreman struct {
	client *elastic.Client
	ctx    context.Context
}

func NewElasticStoreman(client *elastic.Client) *ElasticStoreman {
	return &ElasticStoreman{
		client: client,
		ctx:    context.Background(),
	}
}

func (e *ElasticStoreman) Store(timber Timber) (err error) {

	// TODO: get index predix from timber contenxt
	indexName := fmt.Sprintf("%s-%s-%s",
		INDEX_PREFIX, "location", time.Now().Format("2006.01.02"))

	exists, _ := e.client.IndexExists(indexName).Do(e.ctx)

	if !exists {
		index := createIndex()
		_, err = e.client.CreateIndex(indexName).BodyJson(index).Do(e.ctx)
		if err != nil {
			instru.Count("es_create_index").Event("fail")
			return
		}
		instru.Count("es_create_index").Event("success")
	}

	_, err = e.client.Index().
		Index(indexName).
		Type(MESSAGE_TYPE).
		BodyJson(timber).
		Do(e.ctx)

	if err != nil {
		instru.Count("es_store").Event("fail")
	} else {
		instru.Count("es_store").Event("success")
	}

	return
}

func createIndex() *es.Index {

	return &es.Index{
		Template: fmt.Sprintf("%s-*", INDEX_PREFIX),
		Version:  60001,
		Settings: map[string]interface{}{
			"index.refresh_interval": "5s",
			// "index.read_only_allow_delete": "false",
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
							Type:        "text",
							IgnoreAbove: 256,
						},
					},
				},
			}).
			AddPropertyWithType("@timestamp", "date"),
	}

}
