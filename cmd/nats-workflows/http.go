package main

import (
	"net"
	"os"
	"strings"
)

func httpConfig() (addr, url string) {
	addr = os.Getenv("HTTP_ADDR")
	if port := os.Getenv("PORT"); port != "" {
		addr = net.JoinHostPort(os.Getenv("HOST"), port)
	}
	if addr == "" {
		addr = ":8080"
	}
	if url = os.Getenv("PORTLESS_URL"); url != "" {
		return addr, strings.TrimRight(url, "/")
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, "http://" + addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}
	return addr, "http://" + net.JoinHostPort(host, port)
}
