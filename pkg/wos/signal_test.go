package wos

import (
	"os"
	"runtime"
	"slices"
	"syscall"
	"testing"
	"time"
)

func TestGetSignalsForOS(t *testing.T) {
	for goos, want := range map[string][]os.Signal{
		windowsOS: {syscall.SIGINT, syscall.SIGTERM},
		"linux":   {syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGABRT},
	} {
		if got := getSignalsForOS(goos); !slices.Equal(got, want) {
			t.Errorf("getSignalsForOS(%q) = %v, want %v", goos, got, want)
		}
	}
}

func TestSignal(t *testing.T) {
	ch := Signal()
	if ch == nil {
		t.Error("Signal() returned nil channel")
	}

	// Test that channel is of correct type
	assertSignalChannel(ch)
}

func assertSignalChannel(_ <-chan os.Signal) {}

func TestWaitSignal(t *testing.T) {
	if runtime.GOOS == windowsOS {
		t.Skip("os.Process.Signal does not support SIGINT on Windows")
	}

	done := make(chan struct{})
	go func() {
		WaitSignal()
		close(done)
	}()

	// Send a signal to ourselves
	time.Sleep(100 * time.Millisecond)
	p, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Signal(syscall.SIGINT); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
		// success
	case <-time.After(time.Second):
		t.Error("WaitSignal did not return after receiving signal")
	}
}
