package profile

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func rangeServer(t *testing.T, body string, honourRange bool, status int) *httptest.Server {
	t.Helper()
	return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if status != 0 {
			w.WriteHeader(status)
			return
		}
		if rg := r.Header.Get("Range"); rg != "" && honourRange {
			var from int
			if _, err := fmtSscanf(rg, &from); err != nil {
				http.Error(w, "bad range", 400)
				return
			}
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte(body[from:]))
			return
		}
		_, _ = w.Write([]byte(body))
	}))
}

func fmtSscanf(rg string, from *int) (int, error) {
	n := 0
	rest := strings.TrimSuffix(strings.TrimPrefix(rg, "bytes="), "-")
	for _, c := range rest {
		if c < '0' || c > '9' {
			return 0, os.ErrInvalid
		}
		n = n*10 + int(c-'0')
	}
	*from = n
	return 1, nil
}

// HTTPDownload talks to test servers over TLS; trust their certificate.
func withTestClient(t *testing.T, srv *httptest.Server) {
	t.Helper()
	prev := http.DefaultTransport
	http.DefaultTransport = srv.Client().Transport
	t.Cleanup(func() { http.DefaultTransport = prev })
}

func TestHTTPDownloadFullThenResumes(t *testing.T) {
	srv := rangeServer(t, "0123456789", true, 0)
	defer srv.Close()
	withTestClient(t, srv)
	dst := filepath.Join(t.TempDir(), "f")
	if err := HTTPDownload(context.Background(), srv.URL, dst, 0); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(dst); string(b) != "0123456789" {
		t.Fatalf("%q", b)
	}
	if err := os.WriteFile(dst, []byte("0123"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := HTTPDownload(context.Background(), srv.URL, dst, 4); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(dst); string(b) != "0123456789" {
		t.Fatalf("resume produced %q", b)
	}
}

func TestHTTPDownloadRestartsWhenServerIgnoresRange(t *testing.T) {
	srv := rangeServer(t, "0123456789", false, 0)
	defer srv.Close()
	withTestClient(t, srv)
	dst := filepath.Join(t.TempDir(), "f")
	_ = os.WriteFile(dst, []byte("0123"), 0o644)
	if err := HTTPDownload(context.Background(), srv.URL, dst, 4); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(dst); string(b) != "0123456789" {
		t.Fatalf("a server that ignores Range corrupted the file: %q", b)
	}
}

func TestHTTPDownloadFailsOnErrorStatus(t *testing.T) {
	srv := rangeServer(t, "", true, http.StatusNotFound)
	defer srv.Close()
	withTestClient(t, srv)
	dst := filepath.Join(t.TempDir(), "f")
	if err := HTTPDownload(context.Background(), srv.URL, dst, 0); err == nil || !strings.Contains(err.Error(), "HTTP 404") {
		t.Fatalf("err=%v", err)
	}
	if _, err := os.Stat(dst); err == nil {
		t.Fatal("an error response left a file behind")
	}
}

func TestHTTPDownloadRefusesRedirectToPlainHTTP(t *testing.T) {
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("x")) }))
	defer plain.Close()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, plain.URL, http.StatusFound)
	}))
	defer srv.Close()
	withTestClient(t, srv)
	dst := filepath.Join(t.TempDir(), "f")
	if err := HTTPDownload(context.Background(), srv.URL, dst, 0); err == nil || !strings.Contains(err.Error(), "non-https") {
		t.Fatalf("err=%v", err)
	}
}
