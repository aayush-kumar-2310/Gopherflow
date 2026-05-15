package executors

import "Shared/config"

var runtimeConfig config.Config

func Init(cfg config.Config) {
	runtimeConfig = cfg
}
