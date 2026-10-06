package main

import (
	"context"
	"fmt"
	"log"
	"bytes"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/xrpc"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/repo"
	"github.com/ipfs/go-cid"
)

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

func main() {
	testDIDstr := "did:plc:ylv5w5tfhnkzg2qfdxztd5yx"

	ctx := context.Background()

	pdsEndpoint, err := resolvePDSEndpoint(ctx, testDIDstr)
	if err != nil {
		log.Fatalf("Error resolving PDS endpoint: %v", err)
	}

	// Need to pass in pds endpoint so that xrpc client knows where to make request
	xrpcClient := &xrpc.Client{
		Host: pdsEndpoint,
	}

	// Call get repo to initially backfill.
	carBytes, err := comatproto.SyncGetRepo(ctx, xrpcClient, testDIDstr, "")
	if err != nil {
		log.Fatalf("Failed to download car file: %v", err)
	}

	// Creates an in memory store to read the contents of the car file
	r, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(carBytes))
	if err != nil {
		log.Fatalf("failed to parse CAR: %v", err)
	}

	commit := r.SignedCommit()
	log.Printf("repo DID: %s", commit.Did)
	log.Printf("checkpoint rev: %s", commit.Rev)

	// Walk the Merkle Search Tree and process every record
	count := 0
	err = r.ForEach(ctx, "", func(path string, recordCID cid.Cid) error {
		count++
		if count <= 5 {
			log.Printf("record %d: %s -> %s", count, path, recordCID)
		}
		return nil
	})
	if err != nil {
		log.Fatalf("walking repo failed: %v", err)
	}
	log.Printf("total records: %d", count)

}