package destination

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"

	bencode "github.com/jackpal/bencode-go"
)

const sha1Size = 20

type bencodeTorrent struct {
	Info bencodeInfo `bencode:"info"`
}

type bencodeInfo struct {
	Name        string        `bencode:"name"`
	PieceLength int64         `bencode:"piece length"`
	Pieces      string        `bencode:"pieces"`
	Length      int64         `bencode:"length,omitempty"` // single-file mode
	Files       []bencodeFile `bencode:"files,omitempty"`  // multi-file mode
}

type bencodeFile struct {
	Length int64    `bencode:"length"`
	Path   []string `bencode:"path"`
}

type parsedTorrent struct {
	Name        string
	PieceLength int64
	TotalSize   int64
	NumPieces   int
	PieceHashes []string
	Files       []parsedFile
}

type parsedFile struct {
	Path   string
	Size   int64
	Offset int64
}

func parseTorrentFile(data []byte) (*parsedTorrent, error) {
	var bt bencodeTorrent
	if err := bencode.Unmarshal(bytes.NewReader(data), &bt); err != nil {
		return nil, fmt.Errorf("decoding torrent: %w", err)
	}

	info := bt.Info
	if info.Name == "" {
		return nil, errors.New("torrent has no name")
	}

	rawPieces := info.Pieces
	if len(rawPieces)%sha1Size != 0 {
		return nil, fmt.Errorf("pieces length %d is not a multiple of %d", len(rawPieces), sha1Size)
	}

	numPieces := len(rawPieces) / sha1Size
	pieceHashes := make([]string, numPieces)
	for i := range numPieces {
		pieceHashes[i] = hex.EncodeToString([]byte(rawPieces[i*sha1Size : (i+1)*sha1Size]))
	}

	var files []parsedFile
	var totalSize int64

	if len(info.Files) == 0 {
		// Single-file torrent
		totalSize = info.Length
		files = []parsedFile{{
			Path:   info.Name,
			Size:   info.Length,
			Offset: 0,
		}}
	} else {
		// Multi-file torrent — paths include the torrent name as root directory.
		// In bencode, info.Files[].Path contains components relative to info.Name,
		// e.g. ["Big Buck Bunny.en.srt"]. The full disk path is Name/component.
		var offset int64
		files = make([]parsedFile, len(info.Files))
		for i, f := range info.Files {
			files[i] = parsedFile{
				Path:   filepath.Join(append([]string{info.Name}, f.Path...)...),
				Size:   f.Length,
				Offset: offset,
			}
			offset += f.Length
		}
		totalSize = offset
	}

	return &parsedTorrent{
		Name:        info.Name,
		PieceLength: info.PieceLength,
		TotalSize:   totalSize,
		NumPieces:   numPieces,
		PieceHashes: pieceHashes,
		Files:       files,
	}, nil
}
