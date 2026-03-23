package f2p

import (
	"encoding/json"
	"fmt"
	"io"
)

type ChunkRef struct {
	Key  Key `json:"key"`
	Size int `json:"size"`
}

type ContentManifest struct {
	Version   int        `json:"version"`
	Key       Key        `json:"key"`
	Name      string     `json:"name"`
	Size      int64      `json:"size"`
	ChunkSize int        `json:"chunk_size"`
	Chunks    []ChunkRef `json:"chunks"`
}

type ContentChunk struct {
	Ref  ChunkRef
	Data []byte
}

type ContentBundle struct {
	Manifest ContentManifest
	Chunks   []ContentChunk
}

type DownloadedContent struct {
	Manifest ContentManifest
	Data     []byte
}

type manifestIdentity struct {
	Version   int        `json:"version"`
	Size      int64      `json:"size"`
	ChunkSize int        `json:"chunk_size"`
	Chunks    []ChunkRef `json:"chunks"`
}

func BuildContentBundle(name string, reader io.Reader, chunkSize int) (ContentBundle, error) {
	if name == "" {
		return ContentBundle{}, errContentNameRequired
	}
	if reader == nil {
		return ContentBundle{}, errNilContentReader
	}
	if chunkSize <= 0 {
		chunkSize = defaultChunkSize
	}

	bundle := ContentBundle{}
	buffer := make([]byte, chunkSize)
	for {
		n, err := io.ReadFull(reader, buffer)
		if err == io.EOF {
			break
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			return ContentBundle{}, err
		}
		if n > 0 {
			data := cloneBytes(buffer[:n])
			ref := ChunkRef{Key: HashBytes(data), Size: len(data)}
			bundle.Chunks = append(bundle.Chunks, ContentChunk{Ref: ref, Data: data})
			bundle.Manifest.Chunks = append(bundle.Manifest.Chunks, ref)
			bundle.Manifest.Size += int64(n)
		}
		if err == io.ErrUnexpectedEOF {
			break
		}
	}

	bundle.Manifest.Version = contentManifestVersion
	bundle.Manifest.Name = name
	bundle.Manifest.ChunkSize = chunkSize

	key, err := bundle.Manifest.computedKey()
	if err != nil {
		return ContentBundle{}, err
	}
	bundle.Manifest.Key = key

	return bundle, nil
}

func (m ContentManifest) MarshalBinary() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}

	return json.Marshal(m)
}

func ParseContentManifest(data []byte) (ContentManifest, error) {
	var manifest ContentManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return ContentManifest{}, err
	}
	if err := manifest.Validate(); err != nil {
		return ContentManifest{}, err
	}

	return manifest, nil
}

func (m ContentManifest) Validate() error {
	if m.Version != contentManifestVersion {
		return fmt.Errorf("%w: unsupported manifest version %d", errInvalidManifest, m.Version)
	}
	if m.Name == "" {
		return fmt.Errorf("%w: missing content name", errInvalidManifest)
	}
	if m.ChunkSize <= 0 {
		return fmt.Errorf("%w: invalid chunk size %d", errInvalidManifest, m.ChunkSize)
	}

	var total int64
	for _, chunk := range m.Chunks {
		if chunk.Size <= 0 {
			return fmt.Errorf("%w: invalid chunk size %d", errInvalidManifest, chunk.Size)
		}
		total += int64(chunk.Size)
	}
	if total != m.Size {
		return fmt.Errorf("%w: manifest size mismatch", errInvalidManifest)
	}
	if m.Size > 0 && len(m.Chunks) == 0 {
		return fmt.Errorf("%w: missing chunks for non-empty content", errInvalidManifest)
	}

	expectedKey, err := m.computedKey()
	if err != nil {
		return err
	}
	if m.Key != expectedKey {
		return fmt.Errorf("%w: manifest key mismatch", errInvalidManifest)
	}

	return nil
}

func (m ContentManifest) computedKey() (Key, error) {
	payload, err := json.Marshal(manifestIdentity{
		Version:   m.Version,
		Size:      m.Size,
		ChunkSize: m.ChunkSize,
		Chunks:    m.Chunks,
	})
	if err != nil {
		return Key{}, err
	}

	return HashBytes(payload), nil
}

func validateChunk(ref ChunkRef, data []byte) error {
	if len(data) != ref.Size {
		return fmt.Errorf("%w: chunk size mismatch", errInvalidManifest)
	}
	if HashBytes(data) != ref.Key {
		return fmt.Errorf("%w: chunk hash mismatch", errInvalidManifest)
	}

	return nil
}

func cloneManifest(manifest ContentManifest) ContentManifest {
	cloned := manifest
	cloned.Chunks = append([]ChunkRef(nil), manifest.Chunks...)
	return cloned
}
