package flow

import (
	"testing"

	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/instru"
)

func ResetApplicationSecretCollection() {
	instru.Metric("application_group").Put("app_secrets", nil)
}

func TestContains_NotMatch(t *testing.T) {
	given := []string{"a", "b"}
	exist := Contains(given, "e")

	FatalIf(t, exist, "Should not contain")
}

func TestContains(t *testing.T) {
	given := []string{"a", "b"}
	exist := Contains(given, "b")

	FatalIf(t, !exist, "Should contain")
}

func TestGetApplicationSecretCollection_Empty(t *testing.T) {
	ResetApplicationSecretCollection()
	want := GetApplicationSecretCollection()

	FatalIf(t, len(want) > 0, "Should be empty")
}

func TestGetApplicationSecretCollection_Exist(t *testing.T) {
	expected := []string{"some-secret"}
	ResetApplicationSecretCollection()
	instru.Metric("application_group").Put("app_secrets", expected)
	want := GetApplicationSecretCollection()

	FatalIf(t, len(want) == 0, "Should not be empty")
}

// func TestInstruApplicationSecret(t *testing.T) {
//   appSecret := "some-secret"
//   InstruApplicationSecret(appSecret)
//
//   collection := GetApplicationSecretCollection()
//   FatalIf(t, !Contains(collection, "some-secret"), "Should contain app secret")
// }

func TestInstruApplicationSecret(t *testing.T) {
	appSecret := "some-secret"
	duplicateAppSecret := "some-secret"
	nextAppSecret := "other-secret"

	ResetApplicationSecretCollection()
	InstruApplicationSecret(appSecret)
	collection := GetApplicationSecretCollection()
	FatalIf(t, !Contains(collection, "some-secret"), "Should contain app secret")
	InstruApplicationSecret(duplicateAppSecret)

	collection = GetApplicationSecretCollection()
	FatalIf(t, len(collection) == 2, "Should not be duplicate")

	InstruApplicationSecret(nextAppSecret)
	collection = GetApplicationSecretCollection()
	FatalIf(t, len(collection) > 2, "Should be contain 2 app secret")
}
