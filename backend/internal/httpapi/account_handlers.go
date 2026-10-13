package httpapi

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/auth"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/session"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/store"
)

// sessionNone keeps no session in Sessions.DeleteAll: every session ends.
var sessionNone session.Session

func (s *Server) currentAccount(w http.ResponseWriter, r *http.Request) (store.Account, bool) {
	account, err := s.Store.AccountByID(r.Context(), sessionFrom(r).AccountID)
	if err != nil {
		// The session outlived its account; treat as signed out.
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusUnauthorized, codeUnauthorized, "sign in required")
		} else {
			s.internalError(w, r, err)
		}
		return account, false
	}
	return account, true
}

func (s *Server) getAccount(w http.ResponseWriter, r *http.Request) {
	account, ok := s.currentAccount(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, sessionView{Account: viewAccount(account), CSRFToken: sessionFrom(r).CSRFToken})
}

func (s *Server) updateAccount(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if !decode(w, r, &req) {
		return
	}
	name, ferr := cleanName("name", req.Name, false)
	if ferr != nil {
		writeInvalid(w, ferr)
		return
	}
	account, err := s.Store.UpdateAccountName(r.Context(), sessionFrom(r).AccountID, name)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, viewAccount(account))
}

// changePassword ends every other session of the account.
func (s *Server) changePassword(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CurrentPassword string `json:"current_password"`
		NewPassword     string `json:"new_password"`
	}
	if !decode(w, r, &req) {
		return
	}
	if ferr := validatePassword("new_password", req.NewPassword); ferr != nil {
		writeInvalid(w, ferr)
		return
	}
	account, ok := s.currentAccount(w, r)
	if !ok {
		return
	}
	if account.PasswordHash == nil {
		writeError(w, http.StatusConflict, codeNoPassword, "the account signs in with Telegram and has no password")
		return
	}
	if !s.allow(w, r, s.Options.Limits.LoginAccount, *account.Email) {
		return
	}
	match, err := auth.VerifyPassword(req.CurrentPassword, *account.PasswordHash)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if !match {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: errorDetail{
			Code: codeWrongPassword, Message: "current password is wrong", Field: "current_password",
		}})
		return
	}
	hash, err := auth.HashPassword(req.NewPassword)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if err := s.Store.SetPassword(r.Context(), account.ID, hash); err != nil {
		s.internalError(w, r, err)
		return
	}
	if err := s.Sessions.DeleteAll(r.Context(), account.ID, sessionFrom(r)); err != nil {
		s.internalError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) resendVerification(w http.ResponseWriter, r *http.Request) {
	account, ok := s.currentAccount(w, r)
	if !ok {
		return
	}
	switch {
	case account.Email == nil:
		writeError(w, http.StatusConflict, codeNoEmail, "the account has no email")
		return
	case account.EmailVerifiedAt != nil:
		writeError(w, http.StatusConflict, codeAlreadyVerified, "the email is already confirmed")
		return
	}
	if !s.allow(w, r, s.Options.Limits.VerifyAccount, strconv.FormatInt(account.ID, 10)) {
		return
	}
	s.sendVerification(account.ID, *account.Email)
	w.WriteHeader(http.StatusAccepted)
}

// linkTelegram binds a Telegram account so the user can sign in with either method.
func (s *Server) linkTelegram(w http.ResponseWriter, r *http.Request) {
	user, ok := s.verifyTelegram(w, r)
	if !ok {
		return
	}
	account, err := s.Store.LinkTelegram(r.Context(), sessionFrom(r).AccountID, user.ID, user.Username)
	if errors.Is(err, store.ErrConflict) {
		writeError(w, http.StatusConflict, codeTelegramTaken, "this Telegram account is linked to another Botsight account")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, viewAccount(account))
}
