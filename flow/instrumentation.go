package flow

import "github.com/BaritoLog/instru"

func instruESCreateIndex(err error) {
	if err != nil {
		instru.Count("es_create_index").Event("fail")
	} else {
		instru.Count("es_create_index").Event("success")
	}
}

func instruESStore(err error) {
	if err != nil {
		instru.Count("es_store").Event("fail")
	} else {
		instru.Count("es_store").Event("success")
	}
}
