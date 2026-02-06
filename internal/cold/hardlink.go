package cold

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/arsac/qb-sync/internal/metrics"
	pb "github.com/arsac/qb-sync/proto"
)

// CreateHardlink creates a hardlink from an existing file to a new path.
// This operation is idempotent - if the target already exists and points to
// the same file (same inode), it returns success.
func (s *Server) CreateHardlink(
	ctx context.Context,
	req *pb.CreateHardlinkRequest,
) (*pb.CreateHardlinkResponse, error) {
	sourcePath := filepath.Join(s.config.BasePath, req.GetSourcePath())
	targetPath := filepath.Join(s.config.BasePath, req.GetTargetPath())

	// Check if target already exists
	targetInfo, targetErr := os.Stat(targetPath)
	if targetErr == nil {
		// Target exists - check if it's the same file (idempotent case)
		sourceInfo, sourceErr := os.Stat(sourcePath)
		if sourceErr == nil && os.SameFile(sourceInfo, targetInfo) {
			s.logger.DebugContext(ctx, "hardlink already exists",
				"source", req.GetSourcePath(),
				"target", req.GetTargetPath(),
			)
			return &pb.CreateHardlinkResponse{Success: true}, nil
		}
		// Target exists but is a different file
		return &pb.CreateHardlinkResponse{
			Success: false,
			Error:   "target already exists and is a different file",
		}, nil
	}

	// Ensure target directory exists
	targetDir := filepath.Dir(targetPath)
	if mkdirErr := os.MkdirAll(targetDir, serverDirPermissions); mkdirErr != nil {
		return &pb.CreateHardlinkResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to create target directory: %v", mkdirErr),
		}, nil
	}

	// Create hardlink
	if linkErr := os.Link(sourcePath, targetPath); linkErr != nil {
		// Handle race condition: another goroutine may have created it
		if os.IsExist(linkErr) {
			targetInfo, targetErr = os.Stat(targetPath)
			sourceInfo, sourceErr := os.Stat(sourcePath)
			if targetErr == nil && sourceErr == nil && os.SameFile(sourceInfo, targetInfo) {
				return &pb.CreateHardlinkResponse{Success: true}, nil
			}
		}
		metrics.HardlinkErrorsTotal.WithLabelValues(metrics.ModeCold).Inc()
		return &pb.CreateHardlinkResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to create hardlink: %v", linkErr),
		}, nil
	}

	metrics.HardlinksCreatedTotal.Inc()
	s.logger.InfoContext(ctx, "created hardlink",
		"source", req.GetSourcePath(),
		"target", req.GetTargetPath(),
	)

	return &pb.CreateHardlinkResponse{Success: true}, nil
}

// GetFileByInode checks if a file with the given inode has been registered.
func (s *Server) GetFileByInode(
	_ context.Context,
	req *pb.GetFileByInodeRequest,
) (*pb.GetFileByInodeResponse, error) {
	path, found := s.inodes.GetRegistered(Inode(req.GetInode()))
	return &pb.GetFileByInodeResponse{
		Found: found,
		Path:  path,
	}, nil
}

// RegisterFile registers a completed file's inode for hardlink tracking.
func (s *Server) RegisterFile(
	ctx context.Context,
	req *pb.RegisterFileRequest,
) (*pb.RegisterFileResponse, error) {
	inode := Inode(req.GetInode())
	path := req.GetPath()

	// Verify the file exists and has expected size
	fullPath := filepath.Join(s.config.BasePath, path)
	info, statErr := os.Stat(fullPath)
	if statErr != nil {
		return &pb.RegisterFileResponse{
			Success: false,
			Error:   fmt.Sprintf("file not found: %v", statErr),
		}, nil
	}

	if info.Size() != req.GetSize() {
		return &pb.RegisterFileResponse{
			Success: false,
			Error:   fmt.Sprintf("size mismatch: expected %d, got %d", req.GetSize(), info.Size()),
		}, nil
	}

	s.inodes.Register(inode, path)

	// Persist inode map after registration
	if saveErr := s.inodes.Save(); saveErr != nil {
		s.logger.WarnContext(ctx, "failed to persist inode map", "error", saveErr)
	}

	s.logger.DebugContext(ctx, "registered file for hardlink tracking",
		"inode", inode,
		"path", path,
	)

	return &pb.RegisterFileResponse{Success: true}, nil
}
