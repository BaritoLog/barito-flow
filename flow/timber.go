package flow

// Timber
type Timber map[string]interface{}

func NewTimber() Timber {
	timber := make(map[string]interface{})
	return timber
}

func (t Timber) SetTimestamp(timestamp string) {
	t["@timestamp"] = timestamp
}

func (t Timber) Timestamp() (s string) {
	s, _ = t["@timestamp"].(string)
	return
}

func (t Timber) Context() (ctx *TimberContext) {
	ctx, _ = t["_ctx"].(*TimberContext)
	return
}
