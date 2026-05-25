//go:build system

package system_test

import (
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/mjagos0/datarings/testrig"
)

func TestScenario_LocalBlocks_FilesystemSizeEqualsRawBytes(t *testing.T) {
	const fileSize = 100 * mibibyte

	fileData := genRandomFile(t, fileSize)

	rootCID, dagBlocks := buildDAGBlocks(t, fileData)
	var expectedRawBytes int64
	for _, blk := range dagBlocks {
		expectedRawBytes += int64(blk.Size)
	}

	c := testrig.NewCluster(t, testrig.StateFreshPublic)
	_ = c.Nodes8()
	c.Setup()

	n := c.NodeByName("node1")

	dataDir := n.DataDir()
	if dataDir == "" {
		t.Skip("backend does not expose host data dir (remote test)")
	}

	cidStr, err := n.AddLocal(fileData)
	if err != nil {
		t.Fatalf("AddLocal: %v", err)
	}
	if cidStr != rootCID {
		t.Fatalf("local add CID mismatch: got %s pre-computed %s", cidStr, rootCID)
	}
	t.Logf("[local-fs] added 100 MiB CID=%s (%d DAG blocks)", cidStr, len(dagBlocks))

	blocksDir := filepath.Join(dataDir, "local-blocks")
	dataBytes, dataFiles, otherBytes, otherFiles := classifyDirSize(t, blocksDir)

	t.Logf("[local-fs] sum of raw block bytes (block.Size): %d (%s)",
		expectedRawBytes, formatBytes(expectedRawBytes))
	t.Logf("[local-fs] block-file bytes (.data): %d (%s) across %d file(s)",
		dataBytes, formatBytes(dataBytes), dataFiles)
	t.Logf("[local-fs] flatfs bookkeeping bytes: %d (%s) across %d file(s)",
		otherBytes, formatBytes(otherBytes), otherFiles)

	if dataBytes != expectedRawBytes {
		t.Errorf("[local-fs] block-file bytes %d != raw block bytes %d (delta %d)",
			dataBytes, expectedRawBytes, dataBytes-expectedRawBytes)
	}
	if dataFiles != len(dagBlocks) {
		t.Errorf("[local-fs] block-file count %d != DAG block count %d",
			dataFiles, len(dagBlocks))
	}
}

func classifyDirSize(t *testing.T, dir string) (int64, int, int64, int) {
	t.Helper()
	var dataBytes, otherBytes int64
	var dataFiles, otherFiles int
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return nil
		}
		if filepath.Ext(d.Name()) == ".data" {
			dataBytes += info.Size()
			dataFiles++
		} else {
			otherBytes += info.Size()
			otherFiles++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	return dataBytes, dataFiles, otherBytes, otherFiles
}
