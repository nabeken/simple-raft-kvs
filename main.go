package main

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"

	"github.com/codegangsta/negroni"
)

var (
	ErrNotFound = errors.New("storage: key not found")
)

type Storage interface {
	Get(key []byte) ([]byte, error)
	Set(key []byte, val []byte) error
	Del(key []byte) error
}

type KVSHandler struct {
	storage Storage
}

func (h *KVSHandler) HandleGet(rw http.ResponseWriter, req *http.Request) {
	val, err := h.storage.Get([]byte(req.URL.Path))
	if err != nil {
		switch err {
		case ErrNotFound:
			http.NotFound(rw, req)
		default:
			http.Error(rw, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	io.Copy(rw, bytes.NewReader(val))
}

func (h *KVSHandler) HandlePut(rw http.ResponseWriter, req *http.Request) {
	v, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(v) == 0 {
		http.Error(rw, "size must be larger than 0", http.StatusBadRequest)
		return
	}

	defer req.Body.Close()

	if err := h.storage.Set([]byte(req.URL.Path), v); err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	rw.WriteHeader(http.StatusNoContent)
}

func (h *KVSHandler) HandleDelete(rw http.ResponseWriter, req *http.Request) {
	if err := h.storage.Del([]byte(req.URL.Path)); err != nil {
		switch err {
		case ErrNotFound:
			http.NotFound(rw, req)
		default:
			http.Error(rw, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	rw.WriteHeader(http.StatusNoContent)
}

func (h *KVSHandler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	if req.URL.Path == "/" {
		http.Error(rw, "key must not be empty", http.StatusNotFound)
		return
	}

	switch req.Method {
	case "GET":
		h.HandleGet(rw, req)
	case "PUT":
		h.HandlePut(rw, req)
	case "DELETE":
		h.HandleDelete(rw, req)
	default:
		http.Error(rw, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

func listenAddr() string {
	host := os.Getenv("HOST")
	if host == "" {
		host = "127.0.0.1"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "12345"
	}
	return host + ":" + port
}

func main() {
	db, err := NewLMDB()
	if err != nil {
		log.Fatal(err)
	}

	h := &KVSHandler{
		storage: db,
	}

	defer db.Close()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, os.Kill)

	n := negroni.Classic()
	n.UseHandler(h)
	go n.Run(listenAddr())
	select {
	case <-sigCh:
		log.Print("signal received. existing.")
		return
	}
}
