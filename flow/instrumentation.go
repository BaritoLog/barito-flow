package flow

import (
	"fmt"

	"github.com/BaritoLog/instru"
)

type ApplicationSecretCollection struct {
	appSecrets []string
}

func instruESCreateIndex(err error) {
	if err != nil {
		instru.Count("es_create_index").Event("fail")
	} else {
		instru.Count("es_create_index").Event("success")
	}
}

func instruESStore(appSecret string, err error) {
	InstruApplicationSecret(appSecret)
	label := fmt.Sprintf("%s_es_store", appSecret)
	if err != nil {
		instru.Count(label).Event("fail")
	} else {
		instru.Count(label).Event("success")
	}
}

func Contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func InstruApplicationSecret(appSecret string) {
	appSecrets := GetApplicationSecretCollection()

	if !Contains(appSecrets, appSecret) {
		appSecrets = append(appSecrets, appSecret)
	}

	instru.Metric("application_group").Put("app_secrets", appSecrets)
}

func GetApplicationSecretCollection() []string {
	collection := instru.Metric("application_group").Get("app_secrets")
	if collection == nil {
		return []string{}
	}

	return instru.Metric("application_group").Get("app_secrets").([]string)
}
