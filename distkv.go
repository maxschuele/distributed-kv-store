package main

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
)

type Handler struct {
	m  map[string]string
	rw sync.RWMutex
}

func (h *Handler) handleGet(w http.ResponseWriter, key string) {
	h.rw.RLock()
	val, ok := h.m[key]
	h.rw.Unlock()
	if !ok {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, val)
}

func (h *Handler) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
	}
	defer r.Body.Close()

	h.rw.Lock()
	h.m[key] = string(bodyBytes)
	w.WriteHeader(http.StatusOK)
	h.rw.Unlock()
}

func (h *Handler) handleDelete(w http.ResponseWriter, key string) {
	h.rw.Lock()
	_, ok := h.m[key]
	if ok {
		delete(h.m, key)
	}
	h.rw.Unlock()

	if !ok {
		http.Error(w, "Key not found", http.StatusNotFound)
	}
}

func main() {
	file, err := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	multiWriter := io.MultiWriter(os.Stdout, file)

	handler := slog.NewTextHandler(multiWriter, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})

	logger := slog.New(handler)

	h := &Handler{
		m:  make(map[string]string),
		rw: sync.RWMutex{},
	}

	http.HandleFunc("/api/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		switch r.Method {
		case http.MethodGet:
			h.handleGet(w, key)
		case http.MethodPost:
			h.handlePut(w, r, key)
		case http.MethodDelete:
			h.handleDelete(w, key)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	logger.Info("Starting http server")
	http.ListenAndServe(":8090", nil)
}
