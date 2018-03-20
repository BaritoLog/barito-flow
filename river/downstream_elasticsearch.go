package river

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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

	client, err := elastic.NewClient(elastic.SetURL(conf.Urls))
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
	e.send(indexName, MESSAGE_TYPE, timber)

	return
}

func (e *ElasticsearchDownstream) createIndexIfMissing(indexName string) bool {

	exists, _ := e.client.IndexExists(indexName).Do(e.ctx)

	if !exists {

		var raw string = fmt.Sprintf(`{
		  "template" : "%s-*",
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
		}`, INDEX_PREFIX)

		var indexMapping map[string]interface{}
		json.Unmarshal([]byte(raw), &indexMapping)

		e.client.CreateIndex(indexName).BodyJson(indexMapping).Do(e.ctx)

		return true
	}

	return false
}

func (e *ElasticsearchDownstream) send(indexName, typ string, timber Timber) error {
	var message map[string]interface{}
	err := json.Unmarshal(timber.Data, &message)
	if err != nil {
		message["data"] = timber.Data
	}

	_, err = e.client.Index().Index(indexName).Type(typ).BodyJson(message).Do(e.ctx)

	return err
}
