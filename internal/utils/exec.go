package utils

import (
	"bufio"
	"context"
	"log/slog"
	"os/exec"
)

// Execute runs a command with the given arguments and logs its output.
func Execute(ctx context.Context, logger *slog.Logger, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)

	stdout, stdoutErr := cmd.StdoutPipe()
	if stdoutErr != nil {
		return stdoutErr
	}

	stderr, stderrErr := cmd.StderrPipe()
	if stderrErr != nil {
		return stderrErr
	}

	if startErr := cmd.Start(); startErr != nil {
		return startErr
	}

	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			logger.Info(scanner.Text())
		}
	}()

	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			logger.Warn(scanner.Text())
		}
	}()

	return cmd.Wait()
}
