package main

import "testing"

func TestHTTPConfig(t *testing.T) {
	for _, tc := range []struct {
		name, httpAddr, port, host, publicURL, wantAddr, wantURL string
	}{
		{name: "default", wantAddr: ":8080", wantURL: "http://localhost:8080"},
		{name: "explicit address", httpAddr: "127.0.0.1:9000", wantAddr: "127.0.0.1:9000", wantURL: "http://127.0.0.1:9000"},
		{name: "portless overrides address", httpAddr: ":8080", port: "4321", host: "127.0.0.1", publicURL: "https://nats-workflows.localhost/", wantAddr: "127.0.0.1:4321", wantURL: "https://nats-workflows.localhost"},
		{name: "port without host", port: "4321", wantAddr: ":4321", wantURL: "http://localhost:4321"},
		{name: "IPv6", port: "4321", host: "::1", wantAddr: "[::1]:4321", wantURL: "http://[::1]:4321"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HTTP_ADDR", tc.httpAddr)
			t.Setenv("PORT", tc.port)
			t.Setenv("HOST", tc.host)
			t.Setenv("PORTLESS_URL", tc.publicURL)
			addr, url := httpConfig()
			if addr != tc.wantAddr || url != tc.wantURL {
				t.Fatalf("httpConfig() = %q, %q; want %q, %q", addr, url, tc.wantAddr, tc.wantURL)
			}
		})
	}
}
