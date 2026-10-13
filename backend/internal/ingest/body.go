package ingest

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
)

// MaxBatchBytes is the limit after decompression (Botsight.md, 10.1).
const MaxBatchBytes = 2 << 20

var (
	errTooLarge = errors.New("batch is larger than 2 MiB")
	errBadGzip  = errors.New("invalid gzip body")
	errEncoding = errors.New("unsupported Content-Encoding")
)

var (
	bufPool  = sync.Pool{New: func() any { return new(bytes.Buffer) }}
	gzipPool sync.Pool // *gzip.Reader
)

// readBody reads a possibly gzipped body with limits on both the compressed and the
// decompressed size, so a zip bomb stops at 2 MiB (Botsight.md, 10.2). The caller
// returns the buffer with putBuffer.
func readBody(w http.ResponseWriter, r *http.Request) (*bytes.Buffer, error) {
	body := http.MaxBytesReader(w, r.Body, MaxBatchBytes+64<<10)
	var src io.Reader = body
	switch enc := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Encoding"))); enc {
	case "", "identity":
	case "gzip":
		zr, _ := gzipPool.Get().(*gzip.Reader)
		var err error
		if zr == nil {
			zr, err = gzip.NewReader(body)
		} else {
			err = zr.Reset(body)
		}
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				return nil, errTooLarge
			}
			return nil, errBadGzip
		}
		defer gzipPool.Put(zr)
		src = zr
	default:
		return nil, errEncoding
	}

	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	n, err := buf.ReadFrom(io.LimitReader(src, MaxBatchBytes+1))
	if err != nil {
		putBuffer(buf)
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, errTooLarge
		}
		if errors.Is(err, gzip.ErrChecksum) || errors.Is(err, gzip.ErrHeader) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errBadGzip
		}
		return nil, err
	}
	if n > MaxBatchBytes {
		putBuffer(buf)
		return nil, errTooLarge
	}
	return buf, nil
}

func putBuffer(buf *bytes.Buffer) {
	if buf.Cap() <= 4*MaxBatchBytes {
		bufPool.Put(buf)
	}
}
