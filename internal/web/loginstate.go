package web

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/tg"
)

const (
	loginCookie      = "telfs_login"
	loginSessionTTL  = 10 * time.Minute
	loginWaitTimeout = 30 * time.Second
)

// loginEventKind tags the lifecycle event the login goroutine emits
// after each gotd-driven step. Handlers read these to decide which
// page to render next.
type loginEventKind string

const (
	evCodeSent        loginEventKind = "code_sent"
	evPasswordNeeded  loginEventKind = "password_needed"
	evAcceptingTOS    loginEventKind = "tos"
	evDone            loginEventKind = "done"
)

type loginEvent struct {
	Kind loginEventKind
	Err  error
}

// loginSession is one in-flight phone-login state machine. The gotd
// goroutine drives the flow; HTTP handlers post inputs on the
// channels.
type loginSession struct {
	ID        string
	Profile   string
	Created   time.Time
	LastTouch time.Time

	PhoneCh chan string
	CodeCh  chan string
	PassCh  chan string
	EventCh chan loginEvent

	ctx    context.Context
	cancel context.CancelFunc
}

func newLoginSession(profile string) *loginSession {
	ctx, cancel := context.WithCancel(context.Background())
	return &loginSession{
		ID:        randomHex(16),
		Profile:   profile,
		Created:   time.Now(),
		LastTouch: time.Now(),
		PhoneCh:   make(chan string, 1),
		CodeCh:    make(chan string, 1),
		PassCh:    make(chan string, 1),
		EventCh:   make(chan loginEvent, 4),
		ctx:       ctx,
		cancel:    cancel,
	}
}

// loginRegistry is the in-memory store of active login sessions.
type loginRegistry struct {
	mu       sync.Mutex
	sessions map[string]*loginSession
}

func newLoginRegistry() *loginRegistry {
	r := &loginRegistry{sessions: make(map[string]*loginSession)}
	go r.janitorLoop()
	return r
}

func (r *loginRegistry) Add(s *loginSession) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sessions[s.ID] = s
}

func (r *loginRegistry) Get(id string) *loginSession {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.sessions[id]
	if !ok {
		return nil
	}
	s.LastTouch = time.Now()
	return s
}

func (r *loginRegistry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.sessions[id]; ok {
		s.cancel()
		delete(r.sessions, id)
	}
}

// janitorLoop reaps sessions older than loginSessionTTL (with no
// activity). Calls cancel on each so the gotd goroutine unwinds.
func (r *loginRegistry) janitorLoop() {
	t := time.NewTicker(1 * time.Minute)
	defer t.Stop()
	for range t.C {
		cutoff := time.Now().Add(-loginSessionTTL)
		r.mu.Lock()
		for id, s := range r.sessions {
			if s.LastTouch.Before(cutoff) {
				s.cancel()
				delete(r.sessions, id)
			}
		}
		r.mu.Unlock()
	}
}

// ── webAuth: channel-driven auth.UserAuthenticator ──────────────

// webAuth implements gotd's auth.UserAuthenticator using per-session
// channels. Each method emits an event signaling "I'm waiting for X",
// then blocks on the corresponding input channel until the HTTP
// handler posts the user's response.
type webAuth struct {
	sess *loginSession
}

func (a webAuth) Phone(ctx context.Context) (string, error) {
	select {
	case p := <-a.sess.PhoneCh:
		return p, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (a webAuth) Code(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
	a.emit(loginEvent{Kind: evCodeSent})
	select {
	case c := <-a.sess.CodeCh:
		return c, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (a webAuth) Password(ctx context.Context) (string, error) {
	a.emit(loginEvent{Kind: evPasswordNeeded})
	select {
	case p := <-a.sess.PassCh:
		return p, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (a webAuth) AcceptTermsOfService(_ context.Context, _ tg.HelpTermsOfService) error {
	// Auto-accept. The user agreed to the terms by setting up TelFS
	// against their account; a per-mount popup adds noise without
	// adding signal.
	return nil
}

func (a webAuth) SignUp(_ context.Context) (auth.UserInfo, error) {
	return auth.UserInfo{}, errors.New("sign-up not supported — create the account in the Telegram app first")
}

func (a webAuth) emit(ev loginEvent) {
	select {
	case a.sess.EventCh <- ev:
	default:
		// Channel buffer is 4; if it's full, the handler is asleep —
		// drop the event rather than block the gotd goroutine. This
		// shouldn't happen in normal flow (one event per step).
	}
}

// waitForEvent blocks until the goroutine emits an event or the
// timeout fires. Used by handlers after posting input.
func waitForEvent(s *loginSession, timeout time.Duration) (loginEvent, error) {
	select {
	case ev := <-s.EventCh:
		return ev, nil
	case <-time.After(timeout):
		return loginEvent{}, fmt.Errorf("timeout after %s waiting for telegram", timeout)
	}
}
