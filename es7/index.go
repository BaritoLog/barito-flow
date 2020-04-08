// For elasticsearch 6.2: https://www.elastic.co/guide/en/elasticsearch/reference/6.2/index.html
package es7

type Index struct {
	Settings map[string]interface{} `json:"settings,omitempty"`
	Mappings      *Mappings              `json:"mappings,omitempty"`
}

// NewESIndex
func NewIndex() *Index {
	return &Index{
		Settings: make(map[string]interface{}),
	}
}

func (es *Index) AddSetting(name string, value interface{}) *Index{
	es.Settings[name] = value

	return es
}