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
// the (patched) library does it, plus an independent leak check.
type StatusRecipientReport struct {
	PrivacyType  string   `json:"privacy_type"` // contacts | blacklist | whitelist
	ListCount    int      `json:"list_count"`   // people on the except/only list
	Recipients   int      `json:"recipients"`   // people the status goes to
	Leaked       []string `json:"leaked"`       // excluded people who would still receive it
	Unverifiable int      `json:"unverifiable"` // recipients with no LID while some excluded person has no phone number on file
}

// statusRecipientReport checks every recipient under both its phone-number and
// LID form against the exclude list (also under both forms). The contact store
// and the privacy list do not use the same form, so a literal comparison misses.
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
	if p.Type != types.StatusPrivacyTypeBlacklist || len(p.List) == 0 {
		return rep, nil
	}
	excluded := map[types.JID]string{}
	lidOnly := false // an excluded person known only by LID
	for _, j := range p.List {
		j = j.ToNonAD()
		excluded[j] = j.User
		if alt, err := altJID(ctx, cli, j); err == nil && !alt.IsEmpty() {
			excluded[alt] = j.User
		} else if j.Server == types.HiddenUserServer {
			lidOnly = true
		}
	}
	for _, r := range recipients {
		r = r.ToNonAD()
		alt, aerr := altJID(ctx, cli, r)
		if who, ok := excluded[r]; ok {
			rep.Leaked = append(rep.Leaked, who)
		} else if who, ok := excluded[alt]; aerr == nil && ok {
			rep.Leaked = append(rep.Leaked, who)
		} else if lidOnly && r.Server == types.DefaultUserServer && (aerr != nil || alt.IsEmpty()) {
			rep.Unverifiable++
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
