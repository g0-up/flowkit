package main

import (
	"errors"
	"net/url"
	"strings"
)

// dsnEnvVar names the environment variable read before libpq's PG* variables.
const dsnEnvVar = "FLOWKIT_DATABASE_URL"

var (
	errDSNHasPassword = errors.New("--dsn must not contain a password (it would show in the process list and shell history); " +
		"set PGPASSWORD or use a .pgpass file, or put the whole connection string in " + dsnEnvVar)
	errDSNMalformed = errors.New("--dsn is not a valid connection string")
)

// resolveDSN picks the connection string: --dsn when given, then
// FLOWKIT_DATABASE_URL, then the empty string, which makes pgx read libpq's
// PG* variables and .pgpass. It returns a description of the source for
// messages; the connection string itself is never printed.
func resolveDSN(flagDSN string, getenv func(string) string) (dsn, source string, err error) {
	if flagDSN != "" {
		has, err := dsnHasPassword(flagDSN)
		if err != nil {
			return "", "", err
		}
		if has {
			return "", "", errDSNHasPassword
		}
		return flagDSN, "--dsn", nil
	}
	if v := getenv(dsnEnvVar); v != "" {
		return v, dsnEnvVar, nil
	}
	return "", "PG* environment variables", nil
}

// dsnHasPassword reports whether a URL or keyword/value connection string
// carries a password.
func dsnHasPassword(dsn string) (bool, error) {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			// url.Error repeats the input, so it is not wrapped.
			return false, errDSNMalformed
		}
		if u.User != nil {
			if _, set := u.User.Password(); set {
				return true, nil
			}
		}
		return u.Query().Has("password"), nil
	}
	keys, err := dsnKeys(dsn)
	if err != nil {
		return false, err
	}
	for _, k := range keys {
		if k == "password" {
			return true, nil
		}
	}
	return false, nil
}

// dsnKeys returns the keys of a libpq keyword/value connection string such
// as "host=db user=app dbname='my db'".
func dsnKeys(dsn string) ([]string, error) {
	var keys []string
	s := dsn
	for {
		s = strings.TrimLeft(s, " \t\n\r\f\v")
		if s == "" {
			return keys, nil
		}
		eq := strings.IndexByte(s, '=')
		if eq <= 0 {
			return nil, errDSNMalformed
		}
		key := strings.TrimRight(s[:eq], " \t\n\r\f\v")
		if key == "" || strings.ContainsAny(key, " \t\n\r\f\v'") {
			return nil, errDSNMalformed
		}
		keys = append(keys, strings.ToLower(key))
		s = strings.TrimLeft(s[eq+1:], " \t\n\r\f\v")
		if strings.HasPrefix(s, "'") {
			end, ok := quotedValueEnd(s)
			if !ok {
				return nil, errDSNMalformed
			}
			s = s[end:]
			continue
		}
		end := strings.IndexAny(s, " \t\n\r\f\v")
		if end < 0 {
			return keys, nil
		}
		s = s[end:]
	}
}

// quotedValueEnd returns the index just past the closing quote of the
// single-quoted value at the start of s, honouring backslash escapes.
func quotedValueEnd(s string) (int, bool) {
	for i := 1; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++
		case '\'':
			return i + 1, true
		}
	}
	return 0, false
}
