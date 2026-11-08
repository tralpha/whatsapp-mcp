package main

import (
	"os"
	"os/exec"
	"testing"
)

// TestMediaPreviewVideo: a status video must carry a real first-frame thumbnail,
// size and duration, or the status ring shows a blank tile.
func TestMediaPreviewVideo(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not installed")
	}
	dir := t.TempDir()
	src := dir + "/v.mp4"
	if out, err := exec.Command("ffmpeg", "-v", "error", "-f", "lavfi", "-i", "testsrc=size=1080x1920:rate=30:duration=3",
		"-pix_fmt", "yuv420p", src).CombinedOutput(); err != nil {
		t.Fatalf("make test video: %v %s", err, out)
	}
	data, _ := os.ReadFile(src)
	thumb, w, h, secs := mediaPreview(data, true)
	if len(thumb) < 500 || thumb[0] != 0xFF || thumb[1] != 0xD8 {
		t.Fatalf("thumbnail is not a JPEG (%d bytes)", len(thumb))
	}
	if w != 1080 || h != 1920 || secs != 3 {
		t.Fatalf("got %dx%d %ds, want 1080x1920 3s", w, h, secs)
	}
}

func TestOptU32(t *testing.T) {
	if optU32(0) != nil || *optU32(7) != 7 {
		t.Fatal("optU32")
	}
}
