// For elasticsearch 6.2: https://www.elastic.co/guide/en/elasticsearch/reference/6.2/index.html
package es

type Index struct {
	Template string                 `json:"template,omitempty"`
	Version  int                    `json:"version,omitempty"`
	Settings map[string]interface{} `json:"settings,omitempty"`
	Doc      *Mappings              `json:"_doc,omitempty"`
}

// NewESIndex
func NewIndex() *Index {
	return &Index{
		Settings: make(map[string]interface{}),
	}
}

func (es *Index) AddSetting(name string, value interface{}) {
	es.Settings[name] = value
}
