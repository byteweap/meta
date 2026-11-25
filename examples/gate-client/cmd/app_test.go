package main

import (
	"errors"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"github.com/byteweap/meta/envelope"
)

func TestMainReportsMissingURLAndConnectionError(t *testing.T) {
	const child = "META_GATE_CLIENT_MAIN_TEST"
	if mode := os.Getenv(child); mode != "" {
		flag.CommandLine = flag.NewFlagSet("gate-client", flag.ExitOnError)
		os.Args = []string{"gate-client"}
		if mode == "bad url" {
			os.Args = append(os.Args, "-url", "not-a-websocket-url")
		}
		main()
		return
	}
	for _, test := range []struct{ mode, want string }{
		{"missing url", "-url is required"},
		{"bad url", "malformed ws or wss URL"},
	} {
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestMainReportsMissingURLAndConnectionError$")
		cmd.Env = append(os.Environ(), child+"="+test.mode)
		output, err := cmd.CombinedOutput()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(string(output), test.want) {
			t.Fatalf("%s: err=%v output=%q", test.mode, err, output)
		}
	}
}

func TestEnterGameReportsSuccessBusinessFailureAndDisconnect(t *testing.T) {
	t.Setenv("GATE_DEMO_TOKEN", "local-demo-token")
	for _, test := range []struct{ name, want string }{
		{name: "success"},
		{name: "rejected", want: "enter game failed: 409 full"},
		{name: "disconnect", want: "websocket: close"},
	} {
		t.Run(test.name, func(t *testing.T) {
			upgrader := websocket.Upgrader{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer local-demo-token" {
					t.Error("missing bearer token")
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.Close()
				if deadlineErr := conn.SetReadDeadline(time.Now().Add(time.Second)); deadlineErr != nil {
					t.Error(deadlineErr)
					return
				}
				_, frame, err := conn.ReadMessage()
				request := new(envelope.ClientEnvelope)
				if err != nil {
					t.Error(err)
					return
				}
				if decodeErr := proto.Unmarshal(frame, request); decodeErr != nil || request.GetHeader().GetCmd() != 1 || request.GetService() != "game" {
					t.Errorf("unexpected request %v, err=%v", request, decodeErr)
					return
				}
				if test.name == "disconnect" {
					return
				}
				response := &envelope.ServerEnvelope{Header: request.GetHeader(), Kind: envelope.MsgType_MSG_TYPE_RESPONSE}
				if test.name == "rejected" {
					response.Error = &envelope.ResponseError{Code: 409, Message: "full"}
				}
				data, err := proto.Marshal(response)
				if err != nil {
					t.Error(err)
					return
				}
				if err := conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			err := enterGame("ws" + strings.TrimPrefix(server.URL, "http"))
			if (test.want == "" && err != nil) || (test.want != "" && (err == nil || !strings.Contains(err.Error(), test.want))) {
				t.Fatalf("enterGame() = %v, want %q", err, test.want)
			}
		})
	}
}
