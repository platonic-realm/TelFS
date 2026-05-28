package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"telfs/internal/config"
)

// cmdProfile dispatches `telfs profile {list,show,create,delete,use,export,import}`.
func cmdProfile(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("profile: missing subcommand (list, show, create, delete, use, export, import)")
	}
	switch args[0] {
	case "list":
		return cmdProfileList()
	case "show":
		return cmdProfileShow(args[1:])
	case "create":
		return cmdProfileCreate(args[1:])
	case "delete":
		return cmdProfileDelete(args[1:])
	case "use":
		return cmdProfileUse(args[1:])
	case "export":
		return cmdProfileExport(args[1:])
	case "import":
		return cmdProfileImport(args[1:])
	default:
		return fmt.Errorf("profile: unknown subcommand %q", args[0])
	}
}

func cmdProfileList() error {
	root, err := config.ProfilesRoot()
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		fmt.Println("No profiles yet. Create one with `telfs profile create <name>`.")
		return nil
	}
	if err != nil {
		return err
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() && config.ValidateProfileName(e.Name()) == nil {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		fmt.Println("No profiles yet. Create one with `telfs profile create <name>`.")
		return nil
	}
	active := config.ActiveProfile()
	for _, n := range names {
		mark := "  "
		if n == active {
			mark = "* "
		}
		fmt.Println(mark + n)
	}
	if active == "" {
		fmt.Println("\nNo active profile selected (using legacy ./.telfs/ or $TELFS_DIR).")
	}
	return nil
}

func cmdProfileShow(args []string) error {
	fs := flag.NewFlagSet("profile show", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	dir, err := config.DefaultDir()
	if err != nil {
		return err
	}
	active := config.ActiveProfile()
	if active == "" {
		fmt.Println("Active profile: (none — using legacy path)")
	} else {
		fmt.Printf("Active profile: %s\n", active)
	}
	fmt.Printf("Data dir:       %s\n", dir)
	cfg, err := config.Load()
	if err != nil {
		fmt.Printf("Config load:    %v\n", err)
		return nil
	}
	fmt.Printf("Channel:        %s (id=%d)\n", cfg.Channel.Title, cfg.Channel.ID)
	fmt.Printf("API ID:         %d\n", cfg.APIID)
	if cfg.DC != 0 {
		fmt.Printf("DC:             %d\n", cfg.DC)
	}
	return nil
}

func cmdProfileCreate(args []string) error {
	if len(args) == 0 {
		return errors.New("profile create: usage: profile create <name>")
	}
	name := args[0]
	dir, err := config.ProfileDir(name)
	if err != nil {
		return err
	}
	if _, err := os.Stat(dir); err == nil {
		return fmt.Errorf("profile %q already exists at %s", name, dir)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}
	fmt.Printf("Created profile %q at %s\n", name, dir)
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Printf("  TELFS_PROFILE=%s telfs login           # authenticate this profile\n", name)
	fmt.Printf("  TELFS_PROFILE=%s telfs channel set X   # bind to a channel\n", name)
	fmt.Printf("  telfs profile use %s                  # make this profile the default\n", name)
	return nil
}

func cmdProfileDelete(args []string) error {
	fs := flag.NewFlagSet("profile delete", flag.ContinueOnError)
	force := fs.Bool("yes", false, "skip the confirmation prompt")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		return errors.New("profile delete: usage: profile delete [--yes] <name>")
	}
	name := fs.Arg(0)
	dir, err := config.ProfileDir(name)
	if err != nil {
		return err
	}
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("profile %q not found", name)
	}
	if !*force {
		fmt.Fprintf(os.Stderr, "profile delete: refusing to delete %s without --yes (this removes the SQLite DB and session — channel data is unaffected).\n", dir)
		return errors.New("aborted")
	}
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	fmt.Printf("Deleted profile %q (%s)\n", name, dir)
	return nil
}

func cmdProfileUse(args []string) error {
	if len(args) == 0 {
		return errors.New("profile use: usage: profile use <name>")
	}
	name := args[0]
	dir, err := config.ProfileDir(name)
	if err != nil {
		return err
	}
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("profile %q does not exist (create it with `telfs profile create %s`)", name, name)
	}
	if err := config.SetActiveProfile(name); err != nil {
		return err
	}
	fmt.Printf("Active profile set to %q (%s)\n", name, dir)
	return nil
}

// bundleFiles lists what export/import treats as the FS's portable
// state. Excludes cache/ (rebuildable) and any tempfiles.
var bundleFiles = []string{
	config.ConfigFile,
	config.SessionFile,
	config.DBFile,
}

func cmdProfileExport(args []string) error {
	fs := flag.NewFlagSet("profile export", flag.ContinueOnError)
	prof := fs.String("profile", "", "profile to export (default: active profile)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("profile export: usage: profile export [--profile name] <output.tar.gz>")
	}
	outPath := fs.Arg(0)
	src, err := resolveProfileSrc(*prof)
	if err != nil {
		return err
	}
	// Validate the source has the files we expect to bundle.
	for _, f := range bundleFiles {
		if _, err := os.Stat(filepath.Join(src, f)); err != nil {
			return fmt.Errorf("export: %s missing — has this profile been initialized? (%w)", f, err)
		}
	}
	out, err := os.OpenFile(outPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer out.Close()
	gz := gzip.NewWriter(out)
	tw := tar.NewWriter(gz)
	for _, f := range bundleFiles {
		if err := tarAddFile(tw, filepath.Join(src, f), f); err != nil {
			return fmt.Errorf("export %s: %w", f, err)
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}
	info, _ := os.Stat(outPath)
	fmt.Printf("Exported %d files → %s (%d bytes)\n", len(bundleFiles), outPath, info.Size())
	fmt.Println()
	fmt.Println("This bundle contains MTProto session credentials and (if encryption is enabled)")
	fmt.Println("the salt and canary needed to derive the data key from your passphrase.")
	fmt.Println("Treat it like a private key. Anyone with the bundle + your passphrase has full")
	fmt.Println("read/write access to the filesystem.")
	return nil
}

func cmdProfileImport(args []string) error {
	fs := flag.NewFlagSet("profile import", flag.ContinueOnError)
	prof := fs.String("profile", "default", "destination profile name")
	force := fs.Bool("force", false, "overwrite an existing profile of the same name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("profile import: usage: profile import [--profile name] [--force] <input.tar.gz>")
	}
	bundlePath := fs.Arg(0)
	dst, err := config.ProfileDir(*prof)
	if err != nil {
		return err
	}
	if _, err := os.Stat(dst); err == nil && !*force {
		return fmt.Errorf("import: profile %q already exists at %s (use --force to overwrite)", *prof, dst)
	}
	if err := os.MkdirAll(dst, 0o700); err != nil {
		return err
	}
	in, err := os.Open(bundlePath)
	if err != nil {
		return err
	}
	defer in.Close()
	gz, err := gzip.NewReader(in)
	if err != nil {
		return fmt.Errorf("import: gunzip: %w", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	got := map[string]bool{}
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("import: read tar: %w", err)
		}
		name := strings.TrimPrefix(hdr.Name, "./")
		if !slicesContains(bundleFiles, name) {
			// Skip unknown entries — defensive against path traversal etc.
			continue
		}
		f, err := os.OpenFile(filepath.Join(dst, name), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			return err
		}
		if _, err := io.Copy(f, tr); err != nil {
			f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		got[name] = true
	}
	if len(got) != len(bundleFiles) {
		var missing []string
		for _, f := range bundleFiles {
			if !got[f] {
				missing = append(missing, f)
			}
		}
		return fmt.Errorf("import: bundle is incomplete (missing: %v)", missing)
	}
	fmt.Printf("Imported %d files → profile %q (%s)\n", len(got), *prof, dst)
	fmt.Println()
	fmt.Println("Activate it with:")
	fmt.Printf("  telfs profile use %s\n", *prof)
	fmt.Println("Or run a one-off command with:")
	fmt.Printf("  TELFS_PROFILE=%s telfs mount ~/your/mountpoint\n", *prof)
	return nil
}

func resolveProfileSrc(name string) (string, error) {
	if name == "" {
		return config.DefaultDir()
	}
	return config.ProfileDir(name)
}

// tarAddFile copies a single file into the tar archive under nameInTar.
func tarAddFile(tw *tar.Writer, srcPath, nameInTar string) error {
	info, err := os.Stat(srcPath)
	if err != nil {
		return err
	}
	hdr := &tar.Header{
		Name:    nameInTar,
		Mode:    0o600,
		Size:    info.Size(),
		ModTime: info.ModTime(),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	f, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.Copy(tw, f); err != nil {
		return err
	}
	return nil
}

// slicesContains is a tiny stand-in for slices.Contains so we don't bump
// the minimum Go version unnecessarily.
func slicesContains[T comparable](haystack []T, needle T) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}
