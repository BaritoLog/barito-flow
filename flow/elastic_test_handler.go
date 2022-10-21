package flow

import "net/http"

type ELasticTestHandler struct {
	ExistAPIStatus  int
	CreateAPIStatus int
	PostAPIStatus   int
	ResponseBody    []byte
	CustomHandler   func(w http.ResponseWriter, r *http.Request)
}

func (handler *ELasticTestHandler) getResponseBody() (body []byte) {
	body = handler.ResponseBody
	if body == nil {
		body = []byte(`{}`)
	}
	return
}

func (handler *ELasticTestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if handler.CustomHandler == nil {
		if r.Method == "HEAD" { // check if index exist
			w.WriteHeader(handler.ExistAPIStatus)
		} else if r.Method == "PUT" { // create index
			w.WriteHeader(handler.CreateAPIStatus)
			w.Write(handler.getResponseBody())
		} else if r.Method == "POST" { // post message
			w.WriteHeader(handler.PostAPIStatus)
			w.Write(handler.getResponseBody())
		}
	} else {
		handler.CustomHandler(w, r)
	}
}
