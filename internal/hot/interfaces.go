package hot

import (
	"context"

	"github.com/arsac/qb-sync/internal/streaming"
	pb "github.com/arsac/qb-sync/proto"
)

var _ ColdDestination = (*streaming.GRPCDestination)(nil)

// ColdDestination defines operations the orchestrator needs from the cold server.
type ColdDestination interface {
	CheckTorrentStatus(ctx context.Context, hash string) (*streaming.InitTorrentResult, error)
	FinalizeTorrent(ctx context.Context, hash, savePath, category, tags, saveSubPath string) error
	AbortTorrent(ctx context.Context, hash string, deleteFiles bool) (int32, error)
	StartTorrent(ctx context.Context, hash string, tag string) error
	ClearInitResult(hash string)
	InitTorrent(ctx context.Context, req *pb.InitTorrentRequest) (*streaming.InitTorrentResult, error)
}
