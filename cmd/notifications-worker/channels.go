package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/smtp"
	"net/url"
	"strconv"
	"strings"
)

type channel struct {
	ID       string `json:"id"`
	Kind     string `json:"kind"` // ntfy | gotify | slack | pushover | email
	Enabled  bool   `json:"enabled"`
	URL      string `json:"url,omitempty"`
	Token    string `json:"token,omitempty"`
	User     string `json:"user,omitempty"`
	Title    string `json:"title,omitempty"`
	SMTPHost string `json:"smtpHost,omitempty"`
	SMTPPort int    `json:"smtpPort,omitempty"`
	From     string `json:"from,omitempty"`
	To       string `json:"to,omitempty"`
	Password string `json:"password,omitempty"`
}

func parseChannelsJSONLines(text string) ([]channel, error) {
	out := make([]channel, 0)
	for i, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var c channel
		if err := json.Unmarshal([]byte(line), &c); err != nil {
			return nil, fmt.Errorf("line %d: %w", i+1, err)
		}
		c.Kind = strings.ToLower(strings.TrimSpace(c.Kind))
		if c.ID == "" {
			c.ID = fmt.Sprintf("%s-%d", c.Kind, i+1)
		}
		out = append(out, c)
	}
	return out, nil
}

func channelsJSONLines(list []channel) string {
	if len(list) == 0 {
		return ""
	}
	var b strings.Builder
	for i, c := range list {
		raw, _ := json.Marshal(c)
		if i > 0 {
			b.WriteByte('\n')
		}
		b.Write(raw)
	}
	return b.String()
}

func sendToChannel(c channel, title, body string) error {
	if !c.Enabled {
		return nil
	}
	switch c.Kind {
	case "ntfy":
		return sendNtfy(c, title, body)
	case "gotify":
		return sendGotify(c, title, body)
	case "slack":
		return sendSlack(c, title, body)
	case "pushover":
		return sendPushover(c, title, body)
	case "email":
		return sendEmail(c, title, body)
	default:
		return fmt.Errorf("unknown channel kind %q", c.Kind)
	}
}

func httpFetch(urlStr, method, contentType, body string, headers map[string]string) error {
	params := map[string]any{
		"url":    urlStr,
		"method": method,
		"body":   body,
	}
	if contentType != "" {
		params["contentType"] = contentType
	}
	if len(headers) > 0 {
		params["headers"] = headers
	}
	raw, err := hostCall("fleet.http.fetch", params)
	if err != nil {
		return err
	}
	var out struct {
		Status int    `json:"status"`
		Body   string `json:"body"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return err
	}
	if out.Status >= 400 {
		msg := out.Body
		if len(msg) > 200 {
			msg = msg[:200]
		}
		return fmt.Errorf("HTTP %d: %s", out.Status, msg)
	}
	return nil
}

func sendNtfy(c channel, title, body string) error {
	u := strings.TrimSpace(c.URL)
	if u == "" {
		return fmt.Errorf("ntfy url required")
	}
	headers := map[string]string{}
	if t := strings.TrimSpace(c.Title); t != "" {
		headers["Title"] = t
	} else if title != "" {
		headers["Title"] = title
	}
	if tok := strings.TrimSpace(c.Token); tok != "" {
		headers["Authorization"] = "Bearer " + tok
	}
	return httpFetch(u, "POST", "text/plain; charset=utf-8", body, headers)
}

func sendGotify(c channel, title, body string) error {
	base := strings.TrimRight(strings.TrimSpace(c.URL), "/")
	tok := strings.TrimSpace(c.Token)
	if base == "" || tok == "" {
		return fmt.Errorf("gotify url and token required")
	}
	payload, _ := json.Marshal(map[string]any{
		"title":    firstNonEmpty(c.Title, title, "Fleet"),
		"message":  body,
		"priority": 5,
	})
	u := base + "/message?token=" + url.QueryEscape(tok)
	return httpFetch(u, "POST", "application/json", string(payload), nil)
}

func sendSlack(c channel, title, body string) error {
	u := strings.TrimSpace(c.URL)
	if u == "" {
		return fmt.Errorf("slack webhook url required")
	}
	text := body
	if title != "" {
		text = "*" + title + "*\n" + body
	}
	payload, _ := json.Marshal(map[string]any{"text": text})
	return httpFetch(u, "POST", "application/json", string(payload), nil)
}

func sendPushover(c channel, title, body string) error {
	tok := strings.TrimSpace(c.Token)
	user := strings.TrimSpace(c.User)
	if tok == "" || user == "" {
		return fmt.Errorf("pushover token and user required")
	}
	form := url.Values{}
	form.Set("token", tok)
	form.Set("user", user)
	form.Set("title", firstNonEmpty(c.Title, title, "Fleet"))
	form.Set("message", body)
	return httpFetch("https://api.pushover.net/1/messages.json", "POST", "application/x-www-form-urlencoded", form.Encode(), nil)
}

func sendEmail(c channel, title, body string) error {
	host := strings.TrimSpace(c.SMTPHost)
	to := strings.TrimSpace(c.To)
	from := strings.TrimSpace(c.From)
	if host == "" || to == "" || from == "" {
		return fmt.Errorf("email smtpHost, from, and to required")
	}
	port := c.SMTPPort
	if port <= 0 {
		port = 587
	}
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	subject := firstNonEmpty(c.Title, title, "Fleet")
	msg := strings.Join([]string{
		"From: " + from,
		"To: " + to,
		"Subject: " + subject,
		"MIME-Version: 1.0",
		"Content-Type: text/plain; charset=UTF-8",
		"",
		body,
	}, "\r\n")
	user := strings.TrimSpace(c.User)
	pass := c.Password
	var auth smtp.Auth
	if user != "" {
		auth = smtp.PlainAuth("", user, pass, host)
	}
	return smtp.SendMail(addr, auth, from, []string{to}, []byte(msg))
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
