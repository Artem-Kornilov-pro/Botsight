package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"strings"
)

// API key format (Botsight.md, 10.2): bs_live_<prefix>_<secret>.
// The prefix identifies the key and is stored in plain text; the database
// keeps only SHA-256 of the full key.
const (
	APIKeyEnv       = "live"
	apiKeyPrefixLen = 8
	apiKeySecretLen = 40

	// KeyRevocationChannel is the Redis Pub/Sub channel with prefixes of revoked
	// keys; collectors drop them from their caches (Botsight.md, 10.2).
	KeyRevocationChannel = "keys:revoked"
)

const (
	prefixAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	secretAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
)

var ErrMalformedAPIKey = errors.New("auth: malformed API key")

// APIKey is a freshly generated key. Plain is shown to the user once.
type APIKey struct {
	Plain  string // bs_live_ab12cd34_<secret>
	Prefix string // bs_live_ab12cd34
	Hash   []byte // SHA-256 of Plain
}

// NewAPIKey generates a random API key.
func NewAPIKey() (APIKey, error) {
	id, err := randomString(prefixAlphabet, apiKeyPrefixLen)
	if err != nil {
		return APIKey{}, err
	}
	secret, err := randomString(secretAlphabet, apiKeySecretLen)
	if err != nil {
		return APIKey{}, err
	}
	prefix := "bs_" + APIKeyEnv + "_" + id
	plain := prefix + "_" + secret
	return APIKey{Plain: plain, Prefix: prefix, Hash: HashAPIKey(plain)}, nil
}

// ParseAPIKeyPrefix returns the stored prefix of a key: bs_live_ab12cd34.
func ParseAPIKeyPrefix(key string) (string, error) {
	parts := strings.Split(key, "_")
	if len(parts) != 4 || parts[0] != "bs" || parts[1] == "" ||
		len(parts[2]) != apiKeyPrefixLen || !onlyFrom(parts[2], prefixAlphabet) ||
		len(parts[3]) != apiKeySecretLen || !onlyFrom(parts[3], secretAlphabet) {
		return "", ErrMalformedAPIKey
	}
	return parts[0] + "_" + parts[1] + "_" + parts[2], nil
}

// HashAPIKey is the value stored in api_keys.secret_hash.
func HashAPIKey(key string) []byte {
	sum := sha256.Sum256([]byte(key))
	return sum[:]
}

// VerifyAPIKey compares a presented key with the stored hash in constant time.
func VerifyAPIKey(key string, storedHash []byte) bool {
	return subtle.ConstantTimeCompare(HashAPIKey(key), storedHash) == 1
}

func randomString(alphabet string, n int) (string, error) {
	// Rejection sampling keeps every character equally likely.
	limit := byte(256 - 256%len(alphabet))
	out := make([]byte, 0, n)
	buf := make([]byte, n*2)
	for len(out) < n {
		if _, err := rand.Read(buf); err != nil {
			return "", err
		}
		for _, b := range buf {
			if b < limit {
				out = append(out, alphabet[int(b)%len(alphabet)])
				if len(out) == n {
					break
				}
			}
		}
	}
	return string(out), nil
}

func onlyFrom(s, alphabet string) bool {
	for _, r := range s {
		if !strings.ContainsRune(alphabet, r) {
			return false
		}
	}
	return true
}
