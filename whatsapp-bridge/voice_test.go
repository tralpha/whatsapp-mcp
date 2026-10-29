package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// An m4a / mp3 recording must come out as an Ogg/Opus file the voice-note
// path accepts, with its real duration rather than the 30 s default.
func TestToOpusVoice(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not installed")
	}
	for _, ext := range []string{".m4a", ".mp3"} {
		src := filepath.Join(t.TempDir(), "tone"+ext)
		if out, err := exec.Command("ffmpeg", "-loglevel", "error", "-f", "lavfi", "-i", "sine=frequency=440:duration=3", src).CombinedOutput(); err != nil {
			t.Fatalf("make %s: %v %s", ext, err, out)
		}
		raw, _ := os.ReadFile(src)
		if _, _, err := analyzeOggOpus(raw); err == nil {
			t.Fatalf("%s should not already parse as Ogg/Opus", ext)
		}
		opus, err := toOpusVoice(raw, ext)
		if err != nil {
			t.Fatalf("%s: %v", ext, err)
		}
		secs, wave, err := analyzeOggOpus(opus)
		if err != nil || secs < 2 || secs > 4 || len(wave) == 0 {
			t.Fatalf("%s: secs=%d wave=%d err=%v", ext, secs, len(wave), err)
		}
	}
}
