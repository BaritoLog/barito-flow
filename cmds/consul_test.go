package cmds

//
import (
	"net/http"
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
)

func TestConsulElasticsearch(t *testing.T) {
	ts := NewTestServer(http.StatusOK, []byte(`[
	{
		"ServiceAddress": "172.17.0.1",
		"ServicePort": 5000,
		"ServiceMeta": {
        "http_schema": "https"
    }
	},
	{
		"ServiceAddress": "172.17.0.2",
		"ServicePort": 5001,
		"ServiceMeta": {
        "http_schema": "https"
    }
	},
	{
		"ServiceAddress": "172.17.0.3",
		"ServicePort": 5002,
		"ServiceMeta": {
        "http_schema": "https"
    }
	}
]`))
	defer ts.Close()

	urls, err := consulElasticsearchUrl(ts.URL, "name")
	FatalIfError(t, err)
	FatalIf(t, len(urls) != 3, "return wrong urls")
	FatalIf(t, urls[0] != "https://172.17.0.1:5000", "return wrong urls[0]")
	FatalIf(t, urls[1] != "https://172.17.0.2:5001", "return wrong urls[1]")
	FatalIf(t, urls[2] != "https://172.17.0.3:5002", "return wrong urls[2]")
}

func TestConsulElasticsearch_NoHttpSchema(t *testing.T) {
	ts := NewTestServer(http.StatusOK, []byte(`[
	{
		"ServiceAddress": "172.17.0.3",
		"ServicePort": 5000
	}
]`))
	defer ts.Close()

	urls, err := consulElasticsearchUrl(ts.URL, "name")
	FatalIfError(t, err)
	FatalIf(t, urls[0] != "http://172.17.0.3:5000", "wrong url[0]")
}

func TestConsulElasticsearch_NoService(t *testing.T) {
	ts := NewTestServer(http.StatusOK, []byte(`[]`))
	defer ts.Close()

	_, err := consulElasticsearchUrl(ts.URL, "name")
	FatalIfWrongError(t, err, "No Service")
}

func TestConsulKafkaBorkers(t *testing.T) {
	ts := NewTestServer(http.StatusOK, []byte(`[
  {
    "ServiceAddress": "172.17.0.3",
    "ServicePort": 5000
  },
  {
    "ServiceAddress": "172.17.0.4",
    "ServicePort": 5001
  }
]`))
	defer ts.Close()

	brokers, err := consulKafkaBroker(ts.URL, "name")
	FatalIfError(t, err)
	FatalIf(t, len(brokers) != 2, "return wrong brokers")
	FatalIf(t, brokers[0] != "172.17.0.3:5000", "return wrong brokers[0]")
	FatalIf(t, brokers[1] != "172.17.0.4:5001", "return wrong brokers[1]")
}
