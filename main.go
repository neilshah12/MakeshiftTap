package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"bytes"
	"os"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/sequential"
	"github.com/gorilla/websocket"
	"github.com/ipfs/go-cid"
)

// -------------------- IDENTITY --------------------
func ResolvePDSEndpoint(ctx context.Context, didStr string) (string, error) {
	// Parse string into a type-safe DID
	did, err := syntax.ParseDID(didStr)
	if err != nil {
		return "", fmt.Errorf("invalid DID string %q: %w", didStr, err)	
	}

	// Directories are used in atproto for network related tasks. Here using it for DID resolution.
	dir := identity.DefaultDirectory()
	ident, err := dir.LookupDID(ctx, did)
	if err != nil {
		return "", fmt.Errorf("failed to lookup DID %s: %w", did, err)	
	}

	// Extract the PDS host URL from the DID document
	pdsEndpoint := ident.PDSEndpoint()
	if pdsEndpoint == "" {
		return "", fmt.Errorf("no PDS endpoint found in DID document for %s", did)	
	}

	return pdsEndpoint, nil
}

// -------------------- STATE MACHINE --------------------
type RepoState int

// Defining the states
const (
	StateNew RepoState = iota
	StateBackfilling
	StateSynced
)

// Just for testing a local firehose
type CommitEvent struct {
	DID string
	Rev string
	Since string
	Op  string
}

// State machine
type RepoTracker struct {
	did	   string
	pds	   string
	mu     sync.Mutex
	state  RepoState
	rev    string // latest rev we've processed; "" if none
	buffer []CommitEvent
}

// To be called before "get repo"
func (t *RepoTracker) BeginBackfill() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.state = StateBackfilling
}

// If there's an error, then we need to revert back to starting state
func (t *RepoTracker) AbortBackfill() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.state = StateNew
	t.buffer = nil
}

// FinishBackfill records the checkpoint rev, replays the buffer, and changes state to Synced
func (t *RepoTracker) FinishBackfill(checkpointRev string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.rev = checkpointRev
	for _, evt := range t.buffer {
		t.ApplyLocked(evt)
	}
	t.buffer = nil
	t.state = StateSynced
}

// ** Assumes that Mutex is held
func (t *RepoTracker) ApplyLocked(evt CommitEvent) {
	// Since revs are lexicographically sorted, we can just use string comparison to determine if the event is new.
	if evt.Rev <= t.rev {
		fmt.Printf("skip  rev=%s (already covered, have %s)\n", evt.Rev, t.rev)
		return
	}

	// TODO: We're missing something somehow, so we should re backfill
	if evt.Since != t.rev {
		log.Printf("gap   rev=%s since=%s but we have %s, would need to re-backfill", evt.Rev, evt.Since, t.rev)
		return
	}
	fmt.Printf("apply rev=%s op=%s\n", evt.Rev, evt.Op)
	t.rev = evt.Rev
}

// HandleEvent is called for every live event for this repo.
func (t *RepoTracker) HandleEvent(evt CommitEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()

	switch t.state {
	case StateBackfilling:
		log.Printf("Added to buffer, state backfilling, evt=%+v", evt)
		t.buffer = append(t.buffer, evt)
	case StateSynced:
		log.Printf("Directly handling because backfill complete %+v", evt)
		t.ApplyLocked(evt)
	case StateNew:
		log.Printf("Backfill hasn't started yet, ignoring %+v", evt)
		// TODO: trigger a backfill here asynchronously
	}
}

func (t *RepoTracker) Backfill(ctx context.Context) error {
	// Lock the mutex and open the buffer
	t.BeginBackfill()

	// This runs at the end of the code to return to a retryable state if needed
	ok := false
	defer func() {
		if !ok {
			t.AbortBackfill()
		}
	}()

	// Need to pass in pds endpoint so that xrpc client knows where to make request
	xrpcClient := &xrpc.Client{
		Host: t.pds,
	}

	// Call get repo to initially backfill.
	carBytes, err := comatproto.SyncGetRepo(ctx, xrpcClient, t.did, "")
	if err != nil {
		return fmt.Errorf("get repo: %w", err)
	}

	// Creates an in memory store to read the contents of the car file
	r, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(carBytes))
	if err != nil {
		return fmt.Errorf("parsing car file: %w", err)
	}

	checkpointRev := r.SignedCommit().Rev

	// Emit every record in the checkpoint.
	count := 0
	err = r.ForEach(ctx, "", func(path string, c cid.Cid) error {
		count++
		log.Printf("backfill record: %s -> %s", path, c)
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk repo: %w", err)
	}
	log.Printf("backfilled %d records at rev %s", count, checkpointRev)

	// Replay buffered events (skipping stale ones) and go live.
	t.FinishBackfill(checkpointRev)
	ok = true
	return nil
}

// -------------------- LIVE SYNCING --------------------

// Puts the pds host url in the websocket url format
func pdsStreamURL(pds string) (string, error) {
	u, err := url.Parse(pds)
	if err != nil {
		return "", err
	}
	u.Scheme = "wss"
	u.Path = "/xrpc/com.atproto.sync.subscribeRepos"
	return u.String(), nil
}

func RunPDSStream(ctx context.Context, pds string, trackers map[string]*RepoTracker) error {
	wsURL, err := pdsStreamURL(pds)
	if err != nil {
		return err
	}

	// Connect to the stream first, so if an event comes while backfilling we don't miss it
	con, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, http.Header{})
	if err != nil {
		return fmt.Errorf("dial %s: %w", wsURL, err)
	}
	defer con.Close()
	log.Printf("[%s] connected, tracking %d account(s)", pds, len(trackers))

	// Backfill every tracked account on this pds in a background goroutine
	for _, t := range trackers {
		go func(t *RepoTracker) {
			if err := t.Backfill(ctx); err != nil {
				log.Printf("backfill %s failed: %v", t.did, err)
			}
		}(t)
	}

	rsc := &events.RepoStreamCallbacks{
		RepoCommit: func(evt *comatproto.SyncSubscribeRepos_Commit) error {
			t, ok := trackers[evt.Repo]
			if !ok {
				// someone else's post, ignore.
				return nil
			}
			since := ""
			if evt.Since != nil {
				since = *evt.Since
			}
			ops := make([]string, 0, len(evt.Ops))
			for _, op := range evt.Ops {
				ops = append(ops, fmt.Sprintf("%s %s", op.Action, op.Path))
			}
			t.HandleEvent(CommitEvent{DID: evt.Repo, Rev: evt.Rev, Since: since, Op: strings.Join(ops, ", ")})
			return nil
		},
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	sched := sequential.NewScheduler("tap-"+pds, rsc.EventHandler)
	return events.HandleRepoStream(ctx, con, sched, logger)
}

func main() {
	ctx := context.Background()

	dids := []string{
		"did:plc:ylv5w5tfhnkzg2qfdxztd5yx",
	}

	// Group trackers by PDS.
	byPDS := map[string]map[string]*RepoTracker{}
	for _, did := range dids {
		pds, err := ResolvePDSEndpoint(ctx, did)
		if err != nil {
			log.Printf("skipping %s: %v", did, err)
			continue
		}
		if byPDS[pds] == nil {
			byPDS[pds] = map[string]*RepoTracker{}
		}
		byPDS[pds][did] = &RepoTracker{did: did, pds: pds}
	}

	// One connection per PDS.
	var wg sync.WaitGroup
	for pds, trackers := range byPDS {
		wg.Add(1)
		go func(pds string, trackers map[string]*RepoTracker) {
			defer wg.Done()
			if err := RunPDSStream(ctx, pds, trackers); err != nil {
				log.Printf("[%s] stream ended: %v", pds, err)
			}
		}(pds, trackers)
	}
	wg.Wait()
}