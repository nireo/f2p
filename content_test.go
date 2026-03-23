package f2p

import (
	"bytes"
	"reflect"
	"testing"
)

func TestBuildContentBundleRoundTrip(t *testing.T) {
	data := []byte("hello from the chunked world")

	bundleA, err := BuildContentBundle("first.txt", bytes.NewReader(data), 5)
	if err != nil {
		t.Fatalf("BuildContentBundle returned error: %v", err)
	}
	bundleB, err := BuildContentBundle("second.txt", bytes.NewReader(data), 5)
	if err != nil {
		t.Fatalf("BuildContentBundle returned error: %v", err)
	}

	if bundleA.Manifest.Key != bundleB.Manifest.Key {
		t.Fatalf("manifest keys differ for same content: %x vs %x", bundleA.Manifest.Key, bundleB.Manifest.Key)
	}
	if bundleA.Manifest.Name == bundleB.Manifest.Name {
		t.Fatalf("expected content names to remain distinct")
	}

	encoded, err := bundleA.Manifest.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary returned error: %v", err)
	}

	parsed, err := ParseContentManifest(encoded)
	if err != nil {
		t.Fatalf("ParseContentManifest returned error: %v", err)
	}

	if !reflect.DeepEqual(parsed, bundleA.Manifest) {
		t.Fatalf("parsed manifest = %#v, want %#v", parsed, bundleA.Manifest)
	}
	if len(bundleA.Chunks) == 0 {
		t.Fatalf("expected at least one chunk")
	}
}

func TestPublishAndDownloadContentAcrossNetwork(t *testing.T) {
	bootstrap, stopBootstrap := newRPCNode(t, testKey(0x10))
	defer stopBootstrap()

	peer, stopPeer := newRPCNode(t, testKey(0x20))
	defer stopPeer()

	publisher, stopPublisher := newRPCNode(t, testKey(0x30))
	defer stopPublisher()

	reader, stopReader := newRPCNode(t, testKey(0x40))
	defer stopReader()

	bootstrap.rt.UpdateContact(peer.Info)
	peer.rt.UpdateContact(bootstrap.Info)

	if err := publisher.JoinNetwork(bootstrap.Info); err != nil {
		t.Fatalf("publisher JoinNetwork returned error: %v", err)
	}

	data := []byte("hello from the stdlib-only p2p content model")
	manifest, err := publisher.PublishContentBytes("greeting.txt", data, 8)
	if err != nil {
		t.Fatalf("PublishContentBytes returned error: %v", err)
	}

	if cached, ok := publisher.getManifest(manifest.Key); !ok || !reflect.DeepEqual(cached, manifest) {
		t.Fatalf("publisher did not cache manifest locally: ok=%v manifest=%#v", ok, cached)
	}
	for _, chunk := range manifest.Chunks {
		if cached, ok := publisher.getChunk(chunk.Key); !ok || len(cached) != chunk.Size {
			t.Fatalf("publisher missing local chunk %x", chunk.Key)
		}
	}

	if err := reader.JoinNetwork(bootstrap.Info); err != nil {
		t.Fatalf("reader JoinNetwork returned error: %v", err)
	}

	resolvedManifest, found, err := reader.LookupManifest(manifest.Key)
	if err != nil {
		t.Fatalf("LookupManifest returned error: %v", err)
	}
	if !found {
		t.Fatalf("LookupManifest did not find manifest")
	}
	if !reflect.DeepEqual(resolvedManifest, manifest) {
		t.Fatalf("resolved manifest = %#v, want %#v", resolvedManifest, manifest)
	}

	downloaded, err := reader.DownloadContent(manifest.Key)
	if err != nil {
		t.Fatalf("DownloadContent returned error: %v", err)
	}
	if !bytes.Equal(downloaded.Data, data) {
		t.Fatalf("downloaded data = %q, want %q", downloaded.Data, data)
	}
	if !reflect.DeepEqual(downloaded.Manifest, manifest) {
		t.Fatalf("downloaded manifest = %#v, want %#v", downloaded.Manifest, manifest)
	}

	for _, chunk := range manifest.Chunks {
		if cached, ok := reader.getChunk(chunk.Key); !ok || len(cached) != chunk.Size {
			t.Fatalf("reader did not cache chunk %x", chunk.Key)
		}
	}

	chunkKey := manifest.Chunks[0].Key
	bootstrapHasProvider := containsContact(bootstrap.getProviders(chunkKey), publisher.Info.ID)
	peerHasProvider := containsContact(peer.getProviders(chunkKey), publisher.Info.ID)
	if !bootstrapHasProvider && !peerHasProvider {
		t.Fatalf("network did not retain provider record for chunk %x", chunkKey)
	}
}
