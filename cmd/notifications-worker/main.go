package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      any             `json:"id"`
}

type settings struct {
	Enabled             bool      `json:"enabled"`
	IntervalSec         int       `json:"intervalSec"`
	NotifyAdded         bool      `json:"notifyAdded"`
	NotifyFinished      bool      `json:"notifyFinished"`
	NotifyRemoved       bool      `json:"notifyRemoved"`
	NotifyError         bool      `json:"notifyError"`
	FilterLabel         string    `json:"filterLabel"`
	FilterNameRegex     string    `json:"filterNameRegex"`
	MinSizeMB           int       `json:"minSizeMB"`
	RateLimitMax        int       `json:"rateLimitMax"`
	RateLimitWindowSec  int       `json:"rateLimitWindowSec"`
	TemplateAdded       string    `json:"templateAdded"`
	TemplateFinished    string    `json:"templateFinished"`
	TemplateRemoved     string    `json:"templateRemoved"`
	TemplateError       string    `json:"templateError"`
	Channels            []channel `json:"channels"`
}

type snapRow struct {
	Hash      string  `json:"hash"`
	Name      string  `json:"name"`
	Label     string  `json:"label"`
	State     string  `json:"state"`
	SizeBytes int64   `json:"sizeBytes"`
	Complete  bool    `json:"complete"`
	Ratio     float64 `json:"ratio"`
}

var (
	mu       sync.Mutex
	nextHost = int64(1)
	pending  = map[int64]chan hostReply{}
	outMu    sync.Mutex

	rateMu   sync.Mutex
	rateLog  []time.Time
)

type hostReply struct {
	result json.RawMessage
	err    string
}

func main() {
	go loopScan()
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for in.Scan() {
		line := append([]byte(nil), in.Bytes()...)
		if len(line) == 0 {
			continue
		}
		var probe map[string]json.RawMessage
		if err := json.Unmarshal(line, &probe); err != nil {
			continue
		}
		if _, hasMethod := probe["method"]; !hasMethod {
			var resp struct {
				ID     any             `json:"id"`
				Result json.RawMessage `json:"result"`
				Error  *struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(line, &resp); err != nil {
				continue
			}
			idNum, ok := asInt64(resp.ID)
			if !ok {
				continue
			}
			mu.Lock()
			ch := pending[idNum]
			delete(pending, idNum)
			mu.Unlock()
			if ch == nil {
				continue
			}
			msg := ""
			if resp.Error != nil {
				msg = resp.Error.Message
			}
			ch <- hostReply{result: resp.Result, err: msg}
			continue
		}
		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			continue
		}
		// Handle concurrently so hostCall reverse-RPC can complete while
		// this stdin loop keeps reading host replies.
		go handleRequest(req)
	}
}

func handleRequest(req request) {
	switch req.Method {
	case "fleet.handshake", "fleet.ping", "ext.activate", "ext.deactivate", "ext.health", "ext.upgrade":
		reply(req.ID, map[string]any{"ok": true})
	case "fleet.shutdown":
		reply(req.ID, map[string]any{"ok": true})
		os.Exit(0)
	case "ext.notifications.getSettings":
		st, err := loadSettings()
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, publicSettings(st))
	case "ext.notifications.saveSettings":
		handleSave(req.ID, req.Params)
	case "ext.notifications.test":
		st, err := loadSettings()
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		if err := dispatch(st, "Fleet Notify test", "This is a test notification from org.fleet.notifications"); err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, map[string]any{"ok": true})
	case "ext.notifications.scan":
		res, err := scanOnce()
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, res)
	default:
		replyErr(req.ID, fmt.Sprintf("method not found: %s", req.Method))
	}
}

func handleSave(id any, params json.RawMessage) {
	var p struct {
		Enabled            bool   `json:"enabled"`
		IntervalSec        int    `json:"intervalSec"`
		NotifyAdded        bool   `json:"notifyAdded"`
		NotifyFinished     bool   `json:"notifyFinished"`
		NotifyRemoved      bool   `json:"notifyRemoved"`
		NotifyError        bool   `json:"notifyError"`
		FilterLabel        string `json:"filterLabel"`
		FilterNameRegex    string `json:"filterNameRegex"`
		MinSizeMB          int    `json:"minSizeMB"`
		RateLimitMax       int    `json:"rateLimitMax"`
		RateLimitWindowSec int    `json:"rateLimitWindowSec"`
		TemplateAdded      string `json:"templateAdded"`
		TemplateFinished   string `json:"templateFinished"`
		TemplateRemoved    string `json:"templateRemoved"`
		TemplateError      string `json:"templateError"`
		ChannelsText       string `json:"channelsText"`
	}
	if err := json.Unmarshal(params, &p); err != nil {
		replyErr(id, "invalid settings")
		return
	}
	channels, err := parseChannelsJSONLines(p.ChannelsText)
	if err != nil {
		replyErr(id, err.Error())
		return
	}
	st := settings{
		Enabled:            p.Enabled,
		IntervalSec:        p.IntervalSec,
		NotifyAdded:        p.NotifyAdded,
		NotifyFinished:     p.NotifyFinished,
		NotifyRemoved:      p.NotifyRemoved,
		NotifyError:        p.NotifyError,
		FilterLabel:        p.FilterLabel,
		FilterNameRegex:    p.FilterNameRegex,
		MinSizeMB:          p.MinSizeMB,
		RateLimitMax:       p.RateLimitMax,
		RateLimitWindowSec: p.RateLimitWindowSec,
		TemplateAdded:      p.TemplateAdded,
		TemplateFinished:   p.TemplateFinished,
		TemplateRemoved:    p.TemplateRemoved,
		TemplateError:      p.TemplateError,
		Channels:           channels,
	}
	if err := validateSettings(st); err != nil {
		replyErr(id, err.Error())
		return
	}
	if err := saveSettings(&st); err != nil {
		replyErr(id, err.Error())
		return
	}
	reply(id, publicSettings(st))
}

func loopScan() {
	time.Sleep(3 * time.Second)
	for {
		st, err := loadSettings()
		interval := 15
		if err == nil && st.IntervalSec > 0 {
			interval = st.IntervalSec
		}
		if err == nil && st.Enabled {
			_, _ = scanOnce()
		}
		time.Sleep(time.Duration(interval) * time.Second)
	}
}

func defaultSettings() settings {
	return settings{
		Enabled:            false,
		IntervalSec:        15,
		NotifyAdded:        true,
		NotifyFinished:     true,
		NotifyRemoved:      true,
		NotifyError:        true,
		RateLimitMax:       20,
		RateLimitWindowSec: 300,
		TemplateAdded:      "Added: {name}",
		TemplateFinished:   "Finished: {name}",
		TemplateRemoved:    "Removed: {name}",
		TemplateError:      "Error: {name} ({state})",
		Channels:           []channel{},
	}
}

func publicSettings(st settings) map[string]any {
	return map[string]any{
		"enabled":            st.Enabled,
		"intervalSec":        st.IntervalSec,
		"notifyAdded":        st.NotifyAdded,
		"notifyFinished":     st.NotifyFinished,
		"notifyRemoved":      st.NotifyRemoved,
		"notifyError":        st.NotifyError,
		"filterLabel":        st.FilterLabel,
		"filterNameRegex":    st.FilterNameRegex,
		"minSizeMB":          st.MinSizeMB,
		"rateLimitMax":       st.RateLimitMax,
		"rateLimitWindowSec": st.RateLimitWindowSec,
		"templateAdded":      st.TemplateAdded,
		"templateFinished":   st.TemplateFinished,
		"templateRemoved":    st.TemplateRemoved,
		"templateError":      st.TemplateError,
		"channelsText":       channelsJSONLines(st.Channels),
		"channels":           redactedChannels(st.Channels),
	}
}

func redactedChannels(list []channel) []map[string]any {
	out := make([]map[string]any, 0, len(list))
	for _, c := range list {
		out = append(out, map[string]any{
			"id":      c.ID,
			"kind":    c.Kind,
			"enabled": c.Enabled,
			"url":     c.URL,
			"hasToken": strings.TrimSpace(c.Token) != "" || strings.TrimSpace(c.Password) != "",
		})
	}
	return out
}

func validateSettings(st settings) error {
	if st.IntervalSec < 5 || st.IntervalSec > 3600 {
		return fmt.Errorf("intervalSec must be 5..3600")
	}
	if st.RateLimitMax < 1 {
		st.RateLimitMax = 20
	}
	if st.RateLimitWindowSec < 30 {
		st.RateLimitWindowSec = 300
	}
	if st.FilterNameRegex != "" {
		if _, err := regexp.Compile("(?i)" + st.FilterNameRegex); err != nil {
			return fmt.Errorf("filterNameRegex: %w", err)
		}
	}
	if st.Enabled && len(st.Channels) == 0 {
		return fmt.Errorf("at least one channel required when enabled")
	}
	for _, c := range st.Channels {
		switch c.Kind {
		case "ntfy", "gotify", "slack", "pushover", "email":
		default:
			return fmt.Errorf("channel %s: unsupported kind", c.ID)
		}
	}
	return nil
}

func loadSettings() (settings, error) {
	raw, err := hostCall("fleet.storage.get", map[string]any{"key": "settings"})
	if err != nil {
		return settings{}, err
	}
	if string(raw) == "null" || len(raw) == 0 {
		return defaultSettings(), nil
	}
	var st settings
	if err := json.Unmarshal(raw, &st); err != nil {
		return defaultSettings(), nil
	}
	d := defaultSettings()
	if st.IntervalSec == 0 {
		st.IntervalSec = d.IntervalSec
	}
	if st.RateLimitMax == 0 {
		st.RateLimitMax = d.RateLimitMax
	}
	if st.RateLimitWindowSec == 0 {
		st.RateLimitWindowSec = d.RateLimitWindowSec
	}
	if st.TemplateAdded == "" {
		st.TemplateAdded = d.TemplateAdded
	}
	if st.TemplateFinished == "" {
		st.TemplateFinished = d.TemplateFinished
	}
	if st.TemplateRemoved == "" {
		st.TemplateRemoved = d.TemplateRemoved
	}
	if st.TemplateError == "" {
		st.TemplateError = d.TemplateError
	}
	if st.Channels == nil {
		st.Channels = []channel{}
	}
	return st, nil
}

func saveSettings(st *settings) error {
	_, err := hostCall("fleet.storage.put", map[string]any{"key": "settings", "value": st})
	return err
}

func loadSnap() (map[string]snapRow, error) {
	raw, err := hostCall("fleet.storage.get", map[string]any{"key": "snapshot"})
	if err != nil {
		return nil, err
	}
	out := map[string]snapRow{}
	if string(raw) == "null" || len(raw) == 0 {
		return out, nil
	}
	_ = json.Unmarshal(raw, &out)
	return out, nil
}

func saveSnap(snap map[string]snapRow) error {
	_, err := hostCall("fleet.storage.put", map[string]any{"key": "snapshot", "value": snap})
	return err
}

func scanOnce() (map[string]any, error) {
	st, err := loadSettings()
	if err != nil {
		return nil, err
	}
	raw, err := hostCall("fleet.torrents.list", map[string]any{})
	if err != nil {
		return nil, err
	}
	var payload struct {
		Torrents []snapRow `json:"torrents"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	prev, err := loadSnap()
	if err != nil {
		return nil, err
	}
	cur := map[string]snapRow{}
	sent := 0
	first := len(prev) == 0
	for _, t := range payload.Torrents {
		hash := strings.ToUpper(strings.TrimSpace(t.Hash))
		if hash == "" {
			continue
		}
		t.Hash = hash
		cur[hash] = t
		old, ok := prev[hash]
		if first {
			continue
		}
		if !ok && st.NotifyAdded && passesFilter(st, t) {
			if err := notifyEvent(st, "added", st.TemplateAdded, t); err == nil {
				sent++
			}
		} else if ok && !old.Complete && t.Complete && st.NotifyFinished && passesFilter(st, t) {
			if err := notifyEvent(st, "finished", st.TemplateFinished, t); err == nil {
				sent++
			}
		} else if ok && st.NotifyError && isErrorState(t.State) && !isErrorState(old.State) && passesFilter(st, t) {
			if err := notifyEvent(st, "error", st.TemplateError, t); err == nil {
				sent++
			}
		}
	}
	if !first && st.NotifyRemoved {
		for hash, old := range prev {
			if _, ok := cur[hash]; !ok && passesFilter(st, old) {
				if err := notifyEvent(st, "removed", st.TemplateRemoved, old); err == nil {
					sent++
				}
			}
		}
	}
	if err := saveSnap(cur); err != nil {
		return nil, err
	}
	return map[string]any{"sent": sent, "tracked": len(cur), "prime": first}, nil
}

func isErrorState(state string) bool {
	s := strings.ToLower(state)
	return strings.Contains(s, "error") || strings.Contains(s, "fail")
}

func passesFilter(st settings, t snapRow) bool {
	if lab := strings.TrimSpace(st.FilterLabel); lab != "" {
		if !strings.EqualFold(strings.TrimSpace(t.Label), lab) {
			return false
		}
	}
	if re := strings.TrimSpace(st.FilterNameRegex); re != "" {
		compiled, err := regexp.Compile("(?i)" + re)
		if err != nil || !compiled.MatchString(t.Name) {
			return false
		}
	}
	if st.MinSizeMB > 0 {
		if t.SizeBytes < int64(st.MinSizeMB)*1024*1024 {
			return false
		}
	}
	return true
}

func notifyEvent(st settings, kind, tmpl string, t snapRow) error {
	title := "Fleet " + kind
	body := render(tmpl, t)
	return dispatch(st, title, body)
}

func render(tmpl string, t snapRow) string {
	s := tmpl
	s = strings.ReplaceAll(s, "{name}", t.Name)
	s = strings.ReplaceAll(s, "{hash}", t.Hash)
	s = strings.ReplaceAll(s, "{label}", t.Label)
	s = strings.ReplaceAll(s, "{state}", t.State)
	s = strings.ReplaceAll(s, "{size}", fmt.Sprintf("%d", t.SizeBytes))
	s = strings.ReplaceAll(s, "{ratio}", fmt.Sprintf("%.3f", t.Ratio))
	return s
}

func dispatch(st settings, title, body string) error {
	if !allowSend(st) {
		return fmt.Errorf("rate limited")
	}
	var first error
	ok := 0
	for _, c := range st.Channels {
		if !c.Enabled {
			continue
		}
		if err := sendToChannel(c, title, body); err != nil {
			if first == nil {
				first = err
			}
			continue
		}
		ok++
	}
	if ok == 0 {
		if first != nil {
			return first
		}
		return fmt.Errorf("no enabled channels")
	}
	recordSend()
	return nil
}

func allowSend(st settings) bool {
	max := st.RateLimitMax
	if max < 1 {
		max = 20
	}
	win := time.Duration(st.RateLimitWindowSec) * time.Second
	if win < 30*time.Second {
		win = 5 * time.Minute
	}
	now := time.Now()
	rateMu.Lock()
	defer rateMu.Unlock()
	cut := now.Add(-win)
	kept := rateLog[:0]
	for _, ts := range rateLog {
		if ts.After(cut) {
			kept = append(kept, ts)
		}
	}
	rateLog = kept
	return len(rateLog) < max
}

func recordSend() {
	rateMu.Lock()
	defer rateMu.Unlock()
	rateLog = append(rateLog, time.Now())
}

func hostCall(method string, params any) (json.RawMessage, error) {
	ch := make(chan hostReply, 1)
	mu.Lock()
	id := nextHost
	nextHost++
	pending[id] = ch
	mu.Unlock()
	b, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": method, "params": params, "id": id})
	if err != nil {
		mu.Lock()
		delete(pending, id)
		mu.Unlock()
		return nil, err
	}
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()
	select {
	case reply := <-ch:
		if reply.err != "" {
			return nil, fmt.Errorf("%s", reply.err)
		}
		if len(reply.result) == 0 {
			return json.RawMessage("null"), nil
		}
		return reply.result, nil
	case <-time.After(60 * time.Second):
		mu.Lock()
		delete(pending, id)
		mu.Unlock()
		return nil, fmt.Errorf("host call timeout")
	}
}

func reply(id any, result any) {
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "result": result, "id": id})
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()
}

func replyErr(id any, msg string) {
	b, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"error":   map[string]any{"code": -32000, "message": msg},
		"id":      id,
	})
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()
}

func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	default:
		return 0, false
	}
}
