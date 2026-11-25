package main

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestMainCommand(t *testing.T) {
	const child = "META_CLI_MAIN_TEST_ARG"
	if arg := os.Getenv(child); arg != "" {
		root.SetArgs([]string{arg})
		main()
		return
	}
	for _, tc := range []struct {
		name, arg, want string
		exitCode        int
	}{
		{name: "version", arg: "--version", want: release},
		{name: "invalid flag", arg: "--unknown-for-tests", want: "unknown flag", exitCode: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestMainCommand$")
			cmd.Env = append(os.Environ(), child+"="+tc.arg)
			output, err := cmd.CombinedOutput()
			gotCode := 0
			if err != nil {
				var exit *exec.ExitError
				if !errors.As(err, &exit) {
					t.Fatal(err)
				}
				gotCode = exit.ExitCode()
			}
			if gotCode != tc.exitCode || !strings.Contains(string(output), tc.want) {
				t.Fatalf("exit=%d output=%q, want exit=%d and output containing %q", gotCode, output, tc.exitCode, tc.want)
			}
		})
	}
}
