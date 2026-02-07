package httpserver

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

type Method int

const (
	GET Method = iota
	POST
	PUT
	DELETE
)

var methodMap = map[string]Method{
	"GET":    GET,
	"POST":   POST,
	"PUT":    PUT,
	"DELETE": DELETE,
}

type Request struct {
	Method  Method
	Path    string
	Headers map[string]string
	Body    string
	Params  map[string]string
}

type ResponseWriter struct {
	conn       net.Conn
	statusCode int
	statusText string
	headers    map[string]string
	written    bool
}

type HandlerFunc func(w *ResponseWriter, r *Request)

type Server struct {
	listener *net.TCPListener
	routes   map[Method]map[string]HandlerFunc // method -> path -> handler
}

func New() *Server {
	return &Server{
		routes: make(map[Method]map[string]HandlerFunc),
	}
}

func (s *Server) Bind(addr *net.TCPAddr) error {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var opErr error
			err := c.Control(func(fd uintptr) {
				opErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1)
			})
			if err != nil {
				return err
			}
			return opErr
		},
	}

	listener, err := lc.Listen(context.Background(), "tcp4", addr.String())
	if err != nil {
		return err
	}

	s.listener = listener.(*net.TCPListener)
	return nil
}

func (s *Server) Listen() error {
	if s.listener == nil {
		return fmt.Errorf("No listener is bound. Run Bind(...) first.")
	}

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) RegisterHandler(method Method, path string, handler HandlerFunc) {
	if s.routes[method] == nil {
		s.routes[method] = make(map[string]HandlerFunc)
	}
	s.routes[method][path] = handler
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	req, err := parseRequest(conn)
	if err != nil {
		return
	}

	w := &ResponseWriter{
		conn:       conn,
		statusCode: 200,
		statusText: "OK",
		headers:    make(map[string]string),
	}
	w.Header("Content-Type", "text/plain")
	w.Header("Connection", "close")

	if methods, ok := s.routes[req.Method]; ok {
		if handler, ok := methods[req.Path]; ok {
			handler(w, req)
			if !w.written {
				w.Write(nil)
			}
			return
		}
	}

	w.Status(404, "Not Found")
	w.Write([]byte("404 - Not Found"))
}

func parseRequest(conn net.Conn) (*Request, error) {
	reader := bufio.NewReader(conn)

	requestLine, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	parts := strings.Fields(requestLine)
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid request line")
	}

	fullPath := parts[1]
	path := fullPath
	params := make(map[string]string)

	if before, after, ok := strings.Cut(fullPath, "?"); ok {
		path = before
		queryString := after

		pairs := strings.SplitSeq(queryString, "&")
		for pair := range pairs {
			if kv := strings.SplitN(pair, "=", 2); len(kv) == 2 {
				params[kv[0]] = kv[1]
			}
		}
	}

	req := &Request{
		Method:  Method(methodMap[parts[0]]),
		Path:    path,
		Headers: make(map[string]string),
		Params:  params,
	}

	var contentLength int
	for {
		line, err := reader.ReadString('\n')
		if err != nil || line == "\r\n" {
			break
		}
		if before, after, ok := strings.Cut(line, ":"); ok {
			key := strings.TrimSpace(before)
			value := strings.TrimSpace(after)
			req.Headers[key] = value

			if strings.ToLower(key) == "content-length" {
				fmt.Sscanf(value, "%d", &contentLength)
			}
		}
	}

	if contentLength > 0 {
		body := make([]byte, contentLength)
		reader.Read(body)
		req.Body = string(body)
	}

	return req, nil
}

func (w *ResponseWriter) Status(code int, text string) {
	w.statusCode = code
	w.statusText = text
}

func (w *ResponseWriter) Header(key, value string) {
	w.headers[key] = value
}

func (w *ResponseWriter) Write(body []byte) error {
	if w.written {
		return fmt.Errorf("response already written")
	}
	w.written = true

	var response strings.Builder
	fmt.Fprintf(&response, "HTTP/1.1 %d %s\r\n", w.statusCode, w.statusText)
	w.headers["Content-Length"] = fmt.Sprintf("%d", len(body))

	for key, value := range w.headers {
		fmt.Fprintf(&response, "%s: %s\r\n", key, value)
	}
	response.WriteString("\r\n")

	_, err := w.conn.Write([]byte(response.String()))
	if err != nil {
		return err
	}

	if body != nil {
		_, err = w.conn.Write(body)
	}
	return err
}

func (w *ResponseWriter) JSON(body []byte) error {
	w.Header("Content-Type", "application/json")
	return w.Write(body)
}
