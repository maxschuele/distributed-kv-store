package httpclient

import (
	"bufio"
	"fmt"
	"maps"
	"net"
	"strconv"
	"strings"
)

type Method int

const (
	GET Method = iota
	POST
	PUT
	DELETE
)

var methodNames = map[Method]string{
	GET:    "GET",
	POST:   "POST",
	PUT:    "PUT",
	DELETE: "DELETE",
}

// Request represents an outgoing HTTP request.
type Request struct {
	Method  Method
	Host    string
	Path    string
	Headers map[string]string
	Body    string
}

// Response represents a parsed HTTP response.
type Response struct {
	StatusCode int
	StatusText string
	Headers    map[string]string
	Body       string
}

// Client holds default headers applied to every request.
type Client struct {
	DefaultHeaders map[string]string
}

func NewClient() *Client {
	return &Client{
		DefaultHeaders: map[string]string{
			"Connection": "close",
		},
	}
}

// NewRequest builds a Request. Path should include any query string, e.g. "/users?page=2".
func NewRequest(method Method, host, path string) *Request {
	return &Request{
		Method:  method,
		Host:    host,
		Path:    path,
		Headers: make(map[string]string),
	}
}

// SetBody sets the request body and automatically sets Content-Length.
func (r *Request) SetBody(body string) {
	r.Body = body
	r.Headers["Content-Length"] = strconv.Itoa(len(body))
}

// Do sends the request and returns the parsed response.
// host should be in the form "ip:port" or "hostname:port".
func (c *Client) Do(req *Request) (*Response, error) {
	conn, err := net.Dial("tcp4", req.Host)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	// --- build the raw request -------------------------------------------------
	var buf strings.Builder

	// request line
	fmt.Fprintf(&buf, "%s %s HTTP/1.1\r\n", methodNames[req.Method], req.Path)

	// Host header (required in HTTP/1.1)
	fmt.Fprintf(&buf, "Host: %s\r\n", req.Host)

	// default headers first, then per-request headers (per-request wins)
	merged := make(map[string]string, len(c.DefaultHeaders)+len(req.Headers))
	maps.Copy(merged, c.DefaultHeaders)
	maps.Copy(merged, req.Headers)
	for k, v := range merged {
		fmt.Fprintf(&buf, "%s: %s\r\n", k, v)
	}

	buf.WriteString("\r\n") // end of headers

	if req.Body != "" {
		buf.WriteString(req.Body)
	}

	// --- send ------------------------------------------------------------------
	if _, err := conn.Write([]byte(buf.String())); err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}

	// --- read & parse response -------------------------------------------------
	return parseResponse(conn)
}

func parseResponse(conn net.Conn) (*Response, error) {
	reader := bufio.NewReader(conn)

	// status line: "HTTP/1.1 200 OK\r\n"
	statusLine, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("read status line: %w", err)
	}

	parts := strings.SplitN(strings.TrimSpace(statusLine), " ", 3)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid status line: %q", statusLine)
	}

	statusCode, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("parse status code: %w", err)
	}

	statusText := ""
	if len(parts) == 3 {
		statusText = parts[2]
	}

	// headers
	resp := &Response{
		StatusCode: statusCode,
		StatusText: statusText,
		Headers:    make(map[string]string),
	}

	var contentLength int
	for {
		line, err := reader.ReadString('\n')
		if err != nil || line == "\r\n" {
			break
		}
		if key, value, ok := strings.Cut(line, ":"); ok {
			k := strings.TrimSpace(key)
			v := strings.TrimSpace(value)
			resp.Headers[k] = v

			if strings.EqualFold(k, "content-length") {
				contentLength, _ = strconv.Atoi(v)
			}
		}
	}

	// body
	if contentLength > 0 {
		body := make([]byte, contentLength)
		if _, err := readFull(reader, body); err != nil {
			return nil, fmt.Errorf("read body: %w", err)
		}
		resp.Body = string(body)
	}

	return resp, nil
}

// readFull keeps reading from r until buf is completely filled or an error occurs.
// This is necessary because a single Read call is not guaranteed to return all
// the requested bytes in one go, especially over a network connection.
func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (c *Client) Get(host, path string) (*Response, error) {
	return c.Do(NewRequest(GET, host, path))
}

func (c *Client) Post(host, path, body string) (*Response, error) {
	req := NewRequest(POST, host, path)
	req.SetBody(body)
	return c.Do(req)
}

func (c *Client) Put(host, path, body string) (*Response, error) {
	req := NewRequest(PUT, host, path)
	req.SetBody(body)
	return c.Do(req)
}

func (c *Client) Delete(host, path string) (*Response, error) {
	return c.Do(NewRequest(DELETE, host, path))
}

