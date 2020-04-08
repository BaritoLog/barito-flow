// For elasticsearch 7.6: https://www.elastic.co/guide/en/elasticsearch/reference/7.6/index.html
package es7

type Index struct {
	Settings         map[string]interface{}       `json:"settings,omitempty"`
	DynamicTemplates []map[string]MatchConditions `json:"dynamic_templates"`
	Properties       map[string]Property          `json:"properties"`
}

// NewESIndex
func NewIndex() *Index {
	return &Index{
		Settings: make(map[string]interface{}),
		Properties: make(map[string]Property),
	}
}

func (es *Index) AddSetting(name string, value interface{}) *Index {
	es.Settings[name] = value

	return es
}

type Property struct {
	Type string `json:"type"`
}

// AddDynamicTemplate
func (es *Index) AddDynamicTemplate(name string, matchCondition MatchConditions) *Index {
	m := make(map[string]MatchConditions)
	m[name] = matchCondition

	es.DynamicTemplates = append(es.DynamicTemplates, m)
	return es
}

// AddPropertyWithType
func (es *Index) AddPropertyWithType(name, typ string) *Index {
	es.Properties[name] = Property{Type: typ}
	return es
}
