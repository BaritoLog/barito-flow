package river

import (
	"context"
	"fmt"
	"time"

	"github.com/BaritoLog/barito-flow/es"
	"github.com/BaritoLog/go-boilerplate/errkit"
	"gopkg.in/olivere/elastic.v6"
)

const (
	MESSAGE_TYPE = "fluentd"
	INDEX_PREFIX = "baritolog"
)

type ElasticsearchDownstreamConfig struct {
	Urls string
}

type ElasticsearchDownstream struct {
	client *elastic.Client
	ctx    context.Context
}

func NewElasticsearchDownstream(v interface{}) (Downstream, error) {
	conf, ok := v.(ElasticsearchDownstreamConfig)
	if !ok {
		return nil, errkit.Error("Parameter must be ElasticsearchDownstreamConfig")
	}

	client, err := elastic.NewClient(
		elastic.SetURL(conf.Urls),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
	)
	if err != nil {
		return nil, err
	}

	clientDs := &ElasticsearchDownstream{
		client: client,
		ctx:    context.Background(),
	}

	return clientDs, nil
}

func (e *ElasticsearchDownstream) Store(timber Timber) (err error) {

	indexName := fmt.Sprintf("%s-%s-%s",
		INDEX_PREFIX, timber.Location, time.Now().Format("2006.01.02"))

	e.createIndexIfMissing(indexName)
	err = e.send(indexName, MESSAGE_TYPE, timber)

	return
}

func (e *ElasticsearchDownstream) createIndexIfMissing(indexName string) bool {

	exists, _ := e.client.IndexExists(indexName).Do(e.ctx)

	if !exists {

		index := es.NewIndex()
		index.Template = fmt.Sprintf("%s-*", INDEX_PREFIX)
		index.Version = 60001

		// setting
		index.AddSetting("index.refresh_interval", "5s")
		index.AddSetting("index.read_only_allow_delete", "false")

		// mapping
		index.Doc = es.NewMappings()
		index.Doc.AddDynamicTemplate("message_field", es.MatchConditions{
			PathMatch:        "@message",
			MatchMappingType: "string",
			Mapping: es.MatchMapping{
				Type:  "text",
				Norms: false,
			},
		})
		index.Doc.AddDynamicTemplate("string_fields", es.MatchConditions{
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
		})
		index.Doc.AddPropertyWithType("@timestamp", "date")
		index.Doc.AddPropertyWithType("@version", "keyword")

		e.client.CreateIndex(indexName).BodyJson(index).Do(e.ctx)

		return true
	}

	return false
}

func (e *ElasticsearchDownstream) send(indexName, typ string, timber Timber) error {


	_, err := e.client.Index().Index(indexName).Type(typ).BodyJson(timber).Do(e.ctx)

	return err
}
