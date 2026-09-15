package mocksf

import (
	"encoding/json"
	"net/http"
	"time"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (s *Server) handleToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	if s.faults.FailLogins > 0 {
		s.faults.FailLogins--
		s.mu.Unlock()
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "server_error", "error_description": "injected fault"})
		return
	}
	s.mu.Unlock()

	if err := r.ParseForm(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	switch r.PostForm.Get("grant_type") {
	case "client_credentials":
		if r.PostForm.Get("client_id") != s.opts.ClientId || r.PostForm.Get("client_secret") != s.opts.ClientSecret {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_client", "error_description": "invalid client credentials"})
			return
		}
	case "urn:ietf:params:oauth:grant-type:jwt-bearer":
		if r.PostForm.Get("assertion") == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_grant"})
			return
		}
	case "password":
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported_grant_type", "error_description": "username-password flow is disabled"})
		return
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported_grant_type"})
		return
	}

	tok := s.issueToken()
	writeJSON(w, http.StatusOK, map[string]string{
		"access_token": tok,
		"instance_url": s.opts.BaseURL,
		"id":           s.opts.BaseURL + "/id/" + s.opts.OrgId + "/" + s.opts.UserId,
		"token_type":   "Bearer",
		"issued_at":    time.Now().Format("20060102150405"),
		"signature":    "mock",
	})
}

func (s *Server) handleUserInfo(w http.ResponseWriter, r *http.Request) {
	if !s.validToken(bearer(r)) {
		writeJSON(w, http.StatusUnauthorized, []map[string]string{{"errorCode": "INVALID_SESSION_ID", "message": "Session expired or invalid"}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"user_id":            s.opts.UserId,
		"organization_id":    s.opts.OrgId,
		"preferred_username": "archiver@mock.example",
	})
}
