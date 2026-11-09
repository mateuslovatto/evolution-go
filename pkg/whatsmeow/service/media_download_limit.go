package whatsmeow_service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/net/proxy"
)

// MediaTooLargeError indica que o arquivo passou do teto de mídia. Size é o
// tamanho conhecido (declarado ou, no corte em streaming, pelo menos Max+1).
type MediaTooLargeError struct {
	Size int64
	Max  int64
}

func (e *MediaTooLargeError) Error() string {
	return fmt.Sprintf("media too large: %d bytes exceeds limit of %d bytes", e.Size, e.Max)
}

// ErrMediaTooLarge permite errors.Is sem conhecer o tamanho.
var ErrMediaTooLarge = errors.New("media too large")

func (e *MediaTooLargeError) Is(target error) bool {
	return target == ErrMediaTooLarge
}

type mediaLimitContextKey struct{}

// WithMediaLimit liga o teto de bytes para o download feito com ctx. Sem o
// valor no contexto, o transporte não limita (ex.: mídia no MinIO).
func WithMediaLimit(ctx context.Context, maxBytes int64) context.Context {
	if maxBytes <= 0 {
		return ctx
	}
	return context.WithValue(ctx, mediaLimitContextKey{}, maxBytes)
}

func mediaLimitFrom(ctx context.Context) int64 {
	if ctx == nil {
		return 0
	}
	limit, _ := ctx.Value(mediaLimitContextKey{}).(int64)
	return limit
}

// mediaCryptoOverhead cobre o padding PKCS#7 (até 16 bytes) e o HMAC de 10
// bytes que o payload criptografado soma ao arquivo original.
const mediaCryptoOverhead int64 = 64

type mediaLimitTransport struct {
	base http.RoundTripper
}

func (t mediaLimitTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	resp, err := base.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	if limit := mediaLimitFrom(req.Context()); limit > 0 {
		resp.Body = &cappedReadCloser{
			body:      resp.Body,
			limit:     limit,
			remaining: limit + mediaCryptoOverhead,
		}
	}
	return resp, nil
}

type cappedReadCloser struct {
	body      io.ReadCloser
	limit     int64
	remaining int64
}

func (c *cappedReadCloser) Read(p []byte) (int, error) {
	if c.remaining <= 0 {
		var probe [1]byte
		if n, _ := c.body.Read(probe[:]); n > 0 {
			return 0, &MediaTooLargeError{Size: c.limit + 1, Max: c.limit}
		}
		return 0, io.EOF
	}
	if int64(len(p)) > c.remaining {
		p = p[:c.remaining]
	}
	n, err := c.body.Read(p)
	c.remaining -= int64(n)
	return n, err
}

func (c *cappedReadCloser) Close() error {
	return c.body.Close()
}

// mediaHTTPClient devolve o cliente de mídia do whatsmeow com o limitador de
// bytes (lido do contexto) preservando o proxy da instância. SetMediaHTTPClient
// descarta o proxy já configurado em SetProxyAddress, por isso o transport é
// montado aqui com a mesma configuração.
func mediaHTTPClient(proxyAddress string) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if proxyAddress != "" {
		parsed, err := url.Parse(proxyAddress)
		if err != nil {
			return failClosedMediaClient(fmt.Errorf("invalid media proxy: %w", err))
		}
		switch parsed.Scheme {
		case "http", "https":
			transport.Proxy = http.ProxyURL(parsed)
		case "socks5":
			dialer, err := proxy.FromURL(parsed, &net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			})
			if err != nil {
				return failClosedMediaClient(fmt.Errorf("invalid media proxy: %w", err))
			}
			contextDialer, ok := dialer.(proxy.ContextDialer)
			if !ok {
				return failClosedMediaClient(errors.New("media proxy does not support context dialing"))
			}
			transport.DialContext = contextDialer.DialContext
		default:
			return failClosedMediaClient(fmt.Errorf("unsupported media proxy scheme %q", parsed.Scheme))
		}
	}
	return &http.Client{Transport: mediaLimitTransport{base: transport}}
}

// failClosedMediaClient never dials direct: a proxy that cannot be built makes
// every media request fail instead of silently using the server IP.
func failClosedMediaClient(err error) *http.Client {
	return &http.Client{Transport: mediaLimitTransport{base: failingRoundTripper{err: err}}}
}

type failingRoundTripper struct {
	err error
}

func (f failingRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, f.err
}
