package main

import (
	"errors"
	"fmt"
	"testing"

	"telfs/internal/crypto"
	"telfs/internal/secret"
)

func TestExtractGlobalFlags(t *testing.T) {
	defer func() { secretFrom, secretFD = "", 0 }()
	out, err := extractGlobalFlags([]string{"telfs", "mount", "--secret-from", "keepassxc", "--readonly", "/mnt", "--secret-fd=4"})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(out) != "[telfs mount --readonly /mnt]" || secretFrom != "keepassxc" || secretFD != 4 {
		t.Fatalf("out=%v from=%q fd=%d", out, secretFrom, secretFD)
	}
	for _, bad := range [][]string{
		{"telfs", "--secret-from", "env"},
		{"telfs", "--secret-from"},
		{"telfs", "--secret-fd", "1"},
	} {
		if _, err := extractGlobalFlags(bad); err == nil {
			t.Errorf("%v: want an error", bad)
		}
	}
}

func TestExitCodes(t *testing.T) {
	if exitCode(fmt.Errorf("x: %w", secret.ErrSource)) != 2 {
		t.Error("source failure must exit 2")
	}
	if exitCode(fmt.Errorf("session.enc: %w", crypto.ErrWrongSecret)) != 3 {
		t.Error("wrong secret must exit 3")
	}
	if exitCode(errors.New("other")) != 1 {
		t.Error("other errors exit 1")
	}
}
