package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strconv"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

// StatusRecipientReport is what a status post would reach, computed the same way
// whatsmeow does it (privacy setting + contact store), plus a leak check.
type StatusRecipientReport struct {
	PrivacyType string   `json:"privacy_type"` // contacts | blacklist | whitelist
	ListCount   int      `json:"list_count"`   // people on the except/only list
	Recipients  int      `json:"recipients"`   // devices' owners the status goes to
	Leaked      []string `json:"leaked"`       // excluded people who would still receive it
	Unmapped    int      `json:"unmapped"`     // excluded people with no LID<->phone mapping (checked as-is)
}

// statusRecipientReport flags an excluded contact as leaked if they, or their
// LID/phone-number twin, are in the recipient set. The contact store can hold a
// person under one form while the privacy list names the other, and whatsmeow
// compares them literally.
func statusRecipientReport(ctx context.Context, cli *whatsmeow.Client) (StatusRecipientReport, error) {
	var rep StatusRecipientReport
	privacy, err := cli.GetStatusPrivacy(ctx)
	if err != nil || len(privacy) == 0 {
		return rep, err
	}
	p := privacy[0]
	rep.PrivacyType, rep.ListCount = string(p.Type), len(p.List)
	recipients, err := cli.DangerousInternals().GetStatusBroadcastRecipients(ctx)
	if err != nil {
		return rep, err
	}
	rep.Recipients = len(recipients)
	if p.Type != types.StatusPrivacyTypeBlacklist {
		return rep, nil
	}
	in := make(map[string]bool, len(recipients))
	for _, j := range recipients {
		in[j.ToNonAD().String()] = true
	}
	for _, j := range p.List {
		j = j.ToNonAD()
		alt, aerr := altJID(ctx, cli, j)
		if aerr != nil || alt.IsEmpty() {
			rep.Unmapped++
		}
		if in[j.String()] || (!alt.IsEmpty() && in[alt.String()]) {
			rep.Leaked = append(rep.Leaked, j.User)
		}
	}
	return rep, nil
}

func altJID(ctx context.Context, cli *whatsmeow.Client, j types.JID) (types.JID, error) {
	if j.Server == types.HiddenUserServer {
		return cli.Store.LIDs.GetPNForLID(ctx, j)
	}
	return cli.Store.LIDs.GetLIDForPN(ctx, j)
}

// mediaPreview returns a small JPEG thumbnail (first frame), dimensions and, for
// video, whole seconds. Best effort: zero values if ffmpeg/ffprobe fail.
func mediaPreview(data []byte, isVideo bool) (thumb []byte, w, h, secs uint32) {
	f, err := os.CreateTemp("", "status-media-*")
	if err != nil {
		return
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err != nil {
		f.Close()
		return
	}
	f.Close()

	var probe struct {
		Streams []struct {
			Width  uint32 `json:"width"`
			Height uint32 `json:"height"`
		} `json:"streams"`
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
	}
	if out, err := exec.Command("ffprobe", "-v", "error", "-select_streams", "v:0",
		"-show_entries", "stream=width,height:format=duration", "-of", "json", f.Name()).Output(); err == nil {
		if json.Unmarshal(out, &probe) == nil && len(probe.Streams) > 0 {
			w, h = probe.Streams[0].Width, probe.Streams[0].Height
			if d, err := strconv.ParseFloat(probe.Format.Duration, 64); err == nil && isVideo {
				secs = uint32(d + 0.5)
			}
		}
	}
	var buf bytes.Buffer
	cmd := exec.Command("ffmpeg", "-v", "error", "-i", f.Name(), "-frames:v", "1",
		"-vf", "scale='min(240,iw)':-2", "-q:v", "5", "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1")
	cmd.Stdout = &buf
	if cmd.Run() == nil {
		thumb = buf.Bytes()
	}
	return
}

func optU32(v uint32) *uint32 {
	if v == 0 {
		return nil
	}
	return &v
}
