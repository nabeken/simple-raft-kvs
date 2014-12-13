package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

type KVSHandlerTestSuite struct {
	suite.Suite

	s *LMDB
	h *KVSHandler
}

func (s *KVSHandlerTestSuite) SetupSuite() {
	db, err := NewLMDB()
	if err != nil {
		s.T().Fatal(err)
	}

	s.s = db
	s.h = &KVSHandler{
		storage: db,
	}
}

func (s *KVSHandlerTestSuite) SetupTest() {
}

func (s *KVSHandlerTestSuite) TestEmptyPath() {
	for _, m := range []string{"GET", "PUT", "DELETE"} {
		request(func(rr *httptest.ResponseRecorder) {
			s.h.ServeHTTP(rr, mustRequest(m, "/", nil))
			s.Equal(http.StatusNotFound, rr.Code)
		})
	}
}

func (s *KVSHandlerTestSuite) TestGetNonExistingKey() {
	request(func(rr *httptest.ResponseRecorder) {
		s.h.ServeHTTP(rr, mustRequest("GET", "/_notfound", nil))
		s.Equal(http.StatusNotFound, rr.Code)
	})
}

func (s *KVSHandlerTestSuite) TestDeleteNonExistingKey() {
	request(func(rr *httptest.ResponseRecorder) {
		s.h.ServeHTTP(rr, mustRequest("DELETE", "/_notfound", nil))
		s.Equal(http.StatusNotFound, rr.Code)
	})
}

func (s *KVSHandlerTestSuite) Test() {
	key := "/key1"
	val := "VAL1"

	request(func(rr *httptest.ResponseRecorder) {
		s.h.ServeHTTP(rr, mustRequest("PUT", key, strings.NewReader(val)))
		s.Equal(http.StatusNoContent, rr.Code)
		s.Len(rr.Body.Bytes(), 0)
	})

	request(func(rr *httptest.ResponseRecorder) {
		s.h.ServeHTTP(rr, mustRequest("GET", key, nil))
		s.Equal(http.StatusOK, rr.Code)
		s.Equal(val, rr.Body.String())
	})

	request(func(rr *httptest.ResponseRecorder) {
		s.h.ServeHTTP(rr, mustRequest("DELETE", key, nil))
		s.Equal(http.StatusNoContent, rr.Code)
		s.Len(rr.Body.Bytes(), 0)
	})
}

func (s *KVSHandlerTestSuite) TearDownTest() {
}

func (s *KVSHandlerTestSuite) TearDownSuite() {
	s.s.Close()
}

func mustRequest(method, path string, r io.Reader) *http.Request {
	req, err := http.NewRequest(method, path, r)
	if err != nil {
		panic(err)
	}
	return req
}

func request(f func(rr *httptest.ResponseRecorder)) {
	f(httptest.NewRecorder())
}

func TestKVSHandlerSuite(t *testing.T) {
	suite.Run(t, new(KVSHandlerTestSuite))
}
