package main

import "testing"

func TestParseChannelsJSONLines(t *testing.T) {
	text := `{"kind":"ntfy","enabled":true,"url":"https://ntfy.sh/fleet-lab"}
# comment
{"id":"g1","kind":"gotify","enabled":false,"url":"https://gotify.example","token":"x"}
`
	list, err := parseChannelsJSONLines(text)
	if err != nil || len(list) != 2 {
		t.Fatalf("list=%v err=%v", list, err)
	}
	if list[0].Kind != "ntfy" || list[1].ID != "g1" {
		t.Fatalf("%+v", list)
	}
}

func TestPassesFilter(t *testing.T) {
	st := settings{FilterLabel: "tv", FilterNameRegex: "720p", MinSizeMB: 1}
	ok := snapRow{Name: "Show.720p", Label: "tv", SizeBytes: 2 * 1024 * 1024}
	if !passesFilter(st, ok) {
		t.Fatal("expected pass")
	}
	bad := snapRow{Name: "Show.1080p", Label: "tv", SizeBytes: 2 * 1024 * 1024}
	if passesFilter(st, bad) {
		t.Fatal("regex should block")
	}
}

func TestIsErrorState(t *testing.T) {
	if !isErrorState("error") || isErrorState("downloading") {
		t.Fatal("error state detect")
	}
}
