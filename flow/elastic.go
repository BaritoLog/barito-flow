package flow

import (
	"context"
	"fmt"

	"time"

	"github.com/BaritoLog/barito-flow/es"
	"github.com/olivere/elastic"
)

func elasticNewClient(urls ...string) (*elastic.Client, error) {
	return elastic.NewClient(
		elastic.SetURL(urls...),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
	)
}

func elasticStore(client *elastic.Client, ctx context.Context, timber Timber) (err error) {

	indexPrefix := timber.Context().ESIndexPrefix
	documentType := timber.Context().ESDocumentType
	indexName := fmt.Sprintf("%s-%s", indexPrefix, time.Now().Format("2006.01.02"))

	exists, _ := client.IndexExists(indexName).Do(ctx)

	if !exists {
		index := elasticCreateIndex(indexPrefix)
		_, err = client.CreateIndex(indexName).
			BodyJson(index).Do(ctx)
		instruESCreateIndex(err)
		if err != nil {
			return
		}
	}

	document := ConvertTimberToElasticDocument(timber)

	_, err = client.Index().Index(indexName).Type(documentType).
		BodyJson(document).Do(ctx)
	instruESStore(err)

	return
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
