package config

import (
	"log"

	"github.com/caarlos0/env"
)

type Params struct {
	ID        string `env:"ID"`
	Tagg      int    `env:"T_AGG"      envDefault:"1"`
	Telect    int    `env:"T_ELECT"    envDefault:"1"`
	Rmax      int    `env:"R_MAX"      envDefault:"3"`
	Rwindow   int    `env:"R_WINDOW"   envDefault:"10"`
	Rfull     int    `env:"R_FULL"     envDefault:"6"`
	Threshold int    `env:"THRESHOLD"  envDefault:"5"`
}

func LoadParamsFromEnv() Params {
	var p Params
	if err := env.Parse(&p); err != nil {
		log.Fatalln(err)
	}
	return p
}
