package main

import (
	"context"
	"fmt"
	"log"
	"bytes"
	"sync"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/xrpc"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/repo"
	"github.com/ipfs/go-cid"
)

// -------------------- IDENTITY --------------------
func resolvePDSEndpoint(ctx context.Context, didStr string) (string, error) {
	// Parse string into a type-safe DID
	did, err := syntax.ParseDID(didStr)
	if err != nil {
		return "", fmt.Errorf("invalid DID string %q: %w", didStr, err)	
	}

	// Directories are an atproto convention to handle DID resolution and network related tasks
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
	Op  string
}

// State machine
type RepoTracker struct {
	did	   string
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
		t.applyLocked(evt)
	}
	t.buffer = nil
	t.state = StateSynced
}

// ** assumes that Mutex is held
func (t *RepoTracker) applyLocked(evt CommitEvent) {
	// Since revs are lexicographically sorted, we can just use string comparison to determine if the event is new.
	if evt.Rev <= t.rev {
		fmt.Printf("skip  rev=%s (already covered, have %s)\n", evt.Rev, t.rev)
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
		t.buffer = append(t.buffer, evt)
	case StateSynced:
		t.applyLocked(evt)
	case StateNew:
		// TODO: trigger a backfill here?
	}
}

func (t *RepoTracker) Backfill(ctx context.Context) error {
	// lock the mutex and open the buffer
	t.BeginBackfill()

	// this runs at the end of the code to return to a retryable state if needed
	ok := false
	defer func() {
		if !ok {
			t.AbortBackfill()
		}
	}()

	pdsEndpoint, err := resolvePDSEndpoint(ctx, t.did)
	if err != nil {
		return err
	}

	// Need to pass in pds endpoint so that xrpc client knows where to make request
	xrpcClient := &xrpc.Client{
		Host: pdsEndpoint,
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

func main() {
	testDIDstr := "did:plc:ylv5w5tfhnkzg2qfdxztd5yx"

	ctx := context.Background()

}