package river

import (
	"gopkg.in/olivere/elastic.v6"
	"github.com/BaritoLog/go-boilerplate/errkit"
	"encoding/json"
	"fmt"
	"time"
	ctx "context"
)

var indexMapping string = `{
  "template" : "baritolog-*",
  "version" : 60001,
  "settings" : {
    "index.refresh_interval" : "5s"
  },
  "mappings" : {
    "_default_" : {
      "dynamic_templates" : [ {
        "message_field" : {
          "path_match" : "message",
          "match_mapping_type" : "string",
          "mapping" : {
            "type" : "text",
            "norms" : false
          }
        }
      }, {
        "string_fields" : {
          "match" : "*",
          "match_mapping_type" : "string",
          "mapping" : {
            "type" : "text", "norms" : false,
            "fields" : {
              "keyword" : { "type": "keyword", "ignore_above": 256 }
            }
          }
        }
      } ],
      "properties" : {
        "@timestamp": { "type": "date"},
        "@version": { "type": "keyword"}
      }
    }
  }
}`

type ElasticsearchDownstreamConfig struct {
	Urls string
}

type ElasticsearchDownstream struct {
	client *elastic.Client
}

func NewElasticsearchDownstream(v interface{}) (Downstream, error) {
	conf, ok := v.(ElasticsearchDownstreamConfig)
	if !ok {
		return nil, errkit.Error("Parameter must be ElasticsearchDownstreamConfig")
	}

	client, err := elastic.NewClient(elastic.SetURL(conf.Urls))
	if err != nil {
		return nil, err
	}

	clientDs := &ElasticsearchDownstream{
		client: client,
	}

	return clientDs, nil
}

func (e *ElasticsearchDownstream) Store(timber Timber) (err error) {
	var f,g interface{}
	err = json.Unmarshal(timber.Data, &f)
	if err != nil {
		return
	}
	m := f.(map[string]interface{})

	indexName := fmt.Sprintf("baritolog-%s-%s", timber.Location, time.Now().Format("2006.01.02"))

	exists, err := e.client.IndexExists(indexName).Do(ctx.Background())
	if err != nil {
		return
	}

	err = json.Unmarshal([]byte(indexMapping), &g)
	mapping := g.(map[string]interface{})

	if !exists {
		_, err := e.client.CreateIndex(indexName).BodyJson(mapping).Do(ctx.Background())
		if err != nil {
			// Handle error
			panic(err)
		}
	}

	_, err = e.client.Index().
		Index(indexName).
		Type("fluentd").
		BodyJson(m).
		Do(ctx.Background())

	return
}