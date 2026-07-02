package main

import (
	"flag"

	"github.com/kubev2v/migration-event-streamer/test/e2e/infra"
)

type configuration struct {
	PlannerImage  string
	StreamerImage string
	PodmanSocket  string
}

var (
	cfg          configuration
	infraManager infra.InfraManager
)

func init() {
	flag.StringVar(&cfg.PlannerImage, "planner-image", "", "Migration planner API container image")
	flag.StringVar(&cfg.StreamerImage, "streamer-image", "", "Migration event streamer container image")
	flag.StringVar(&cfg.PodmanSocket, "podman-socket", "unix:///run/user/1000/podman/podman.sock", "Podman socket path")
}
