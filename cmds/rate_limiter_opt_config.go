package cmds

import "strings"

type RateLimiterOpt string

func NewRateLimiterOpt(s string) RateLimiterOpt {
	switch strings.TrimSpace(strings.ToUpper(s)) {
	case "LOCAL":
		return RateLimiterOptLocal
	case "REDIS":
		return RateLimiterOptRedis
	case "GUBERNATOR":
		return RateLimiterOptGubernator
	}

	return RateLimiterOptUndefined
}

func (r RateLimiterOpt) String() string {
	return string(r)
}

const (
	RateLimiterOptUndefined  RateLimiterOpt = "UNDEFINED"
	RateLimiterOptLocal      RateLimiterOpt = "LOCAL"
	RateLimiterOptRedis      RateLimiterOpt = "REDIS"
	RateLimiterOptGubernator RateLimiterOpt = "GUBERNATOR"
)

var (
	RateLimiterAllowedOpts = []RateLimiterOpt{
		RateLimiterOptLocal, RateLimiterOptRedis, RateLimiterOptGubernator,
	}
)
