package main

import (
	"reflect"
	"testing"
)

func TestResolveResetTopics(t *testing.T) {
	configured := []string{"/event/LoginEventStream", "/event/ApiEventStream"}
	cases := []struct {
		arg  string
		want []string
		ok   bool
	}{
		{"all", configured, true},
		{"/event/LoginEventStream", []string{"/event/LoginEventStream"}, true},
		{" /event/ApiEventStream , /event/LoginEventStream ", []string{"/event/ApiEventStream", "/event/LoginEventStream"}, true},
		{"/event/LoginEventStrem", nil, false}, // typo
		{"/event/LoginEventStream,/event/Nope", nil, false},
		{",", nil, false},
	}
	for _, c := range cases {
		got, err := resolveResetTopics(configured, c.arg)
		if (err == nil) != c.ok {
			t.Errorf("%q: ok=%v err=%v", c.arg, c.ok, err)
			continue
		}
		if c.ok && !reflect.DeepEqual(got, c.want) {
			t.Errorf("%q: got %v, want %v", c.arg, got, c.want)
		}
	}
}
