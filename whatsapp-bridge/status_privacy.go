package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

// StatusRecipientReport is what a status post would reach, computed the same way
// the (patched) library does it, plus an independent leak check.
type StatusRecipientReport struct {
	PrivacyType   string   `json:"privacy_type"`   // contacts | blacklist | whitelist
	ListCount     int      `json:"list_count"`     // people on the except/only list
	Recipients    int      `json:"recipients"`     // people the status goes to
	Leaked        []string `json:"leaked"`         // excluded people who would still receive it
	Unverifiable  int      `json:"unverifiable"`   // recipients with no LID while some excluded person has no phone number on file
	Withheld      int      `json:"withheld"`       // phone-number contacts held back because their LID is unknown
	Unresolved    []string `json:"unresolved"`     // excluded people whose LID/phone twin is unknown (give their numbers via STATUS_RESOLVE_PN)
	ExtraExcluded int      `json:"extra_excluded"` // numbers in STATUS_EXTRA_EXCLUDE
}

// statusRecipientReport checks every recipient under both its phone-number and
// LID form against the exclude list (also under both forms). The contact store
// and the privacy list do not use the same form, so a literal comparison misses.
func statusRecipientReport(ctx context.Context, cli *whatsmeow.Client) (StatusRecipientReport, error) {
	var rep StatusRecipientReport
	resolveConfiguredPNs(ctx, cli)
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
	extra := statusExtraExclude(ctx)
	if p.Type != types.StatusPrivacyTypeBlacklist && len(extra) == 0 {
		return rep, nil
	}
	src := extra
	if p.Type == types.StatusPrivacyTypeBlacklist {
		src = append(append([]types.JID{}, p.List...), extra...)
	}
	rep.ExtraExcluded = len(extra)
	excluded := map[types.JID]string{}
	lidOnly := false // an excluded person whose other form is unknown
	for _, j := range src {
		j = j.ToNonAD()
		excluded[j] = j.User
		if alt, err := altJID(ctx, cli, j); err == nil && !alt.IsEmpty() {
			excluded[alt] = j.User
		} else {
			lidOnly = true // either form unknown
			rep.Unresolved = append(rep.Unresolved, j.String())
		}
	}
	if lidOnly {
		if contacts, err := cli.Store.Contacts.GetAllContacts(ctx); err == nil {
			for c, info := range contacts {
				c = c.ToNonAD()
				if info.FullName == "" {
					continue
				}
				if alt, err := altJID(ctx, cli, c); err != nil || alt.IsEmpty() {
					rep.Withheld++
				}
			}
		}
	}
	for _, r := range recipients {
		r = r.ToNonAD()
		alt, aerr := altJID(ctx, cli, r)
		if who, ok := excluded[r]; ok {
			rep.Leaked = append(rep.Leaked, who)
		} else if who, ok := excluded[alt]; aerr == nil && ok {
			rep.Leaked = append(rep.Leaked, who)
		} else if lidOnly && (aerr != nil || alt.IsEmpty()) {
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

// resolveConfiguredPNs looks up the LID of each phone number in STATUS_RESOLVE_PN
// (comma-separated, digits only) that has no stored LID, and stores the pair.
// Meant for the handful of excluded people the store cannot match; a few
// numbers per call, never the whole contact list (bulk usync is rate limited).
func resolveConfiguredPNs(ctx context.Context, cli *whatsmeow.Client) {
	var missing []types.JID
	for _, n := range strings.Split(os.Getenv("STATUS_RESOLVE_PN")+","+os.Getenv("STATUS_EXTRA_EXCLUDE"), ",") {
		n = strings.Trim(strings.TrimSpace(n), "+")
		if n == "" {
			continue
		}
		pn := types.NewJID(n, types.DefaultUserServer)
		if alt, err := altJID(ctx, cli, pn); err != nil || alt.IsEmpty() {
			missing = append(missing, pn)
		}
	}
	if len(missing) == 0 || len(missing) > 50 {
		return
	}
	infos, err := cli.GetUserInfo(ctx, missing)
	if err != nil {
		fmt.Println("STATUS_RESOLVE_PN lookup failed:", err)
		return
	}
	for pn, info := range infos {
		if !info.LID.IsEmpty() {
			cli.StoreLIDPNMapping(ctx, info.LID, pn)
		}
	}
}

// statusExtraExclude parses STATUS_EXTRA_EXCLUDE (comma-separated phone numbers):
// people hidden from every status this bridge posts, on top of the phone's
// "my contacts except" list. Read on every call, so a variable change applies
// as soon as Railway restarts the bridge.
func statusExtraExclude(_ context.Context) []types.JID {
	var out []types.JID
	for _, n := range strings.Split(os.Getenv("STATUS_EXTRA_EXCLUDE"), ",") {
		n = strings.Trim(strings.TrimSpace(n), "+")
		if n != "" {
			out = append(out, types.NewJID(n, types.DefaultUserServer))
		}
	}
	return out
}

func init() { whatsmeow.StatusExtraExclude = statusExtraExclude }
