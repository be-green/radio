# radio

An interactive terminal radio player for macOS. Browse a curated set of stations
(BBC, FIP, KEXP, KCRW, NTS, WFMU, and more) in a TUI, with a live
waveform meter next to the playing station, pause/resume, and volume control.

It's a single ~640 KB Rust binary that shells out to `ffmpeg` for streaming and
decoding.

## Requirements

- **macOS** — audio goes through `audiotoolbox` and volume control uses
  `osascript`, so this is macOS-only.
- **[ffmpeg](https://ffmpeg.org/)** on your `PATH` at runtime. Install with
  [Homebrew](https://brew.sh/):
  ```sh
  brew install ffmpeg
  ```
- **[Rust toolchain](https://rustup.rs/)** to build from source (`cargo`).
  ```sh
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ```

## Install

The recommended path. From the repo root:

```sh
cargo install --path .
```

This builds in release mode and installs the `radio` binary into `~/.cargo/bin`,
which `rustup` already adds to your `PATH`. After it finishes, `radio` is
available as a global command from any directory.

Verify:

```sh
radio --help
```

### Alternative: build without installing

If you'd rather not install globally, build the binary in place:

```sh
cargo build --release
./target/release/radio
```

The binary is self-contained at `target/release/radio` — you can copy it
anywhere on your `PATH` manually (e.g. `cp target/release/radio /usr/local/bin/`).

## Usage

```sh
radio                 # launch the interactive TUI
radio --list          # list all stations grouped by category
radio "kexp"          # play a station directly by fuzzy name match
radio --help          # show usage
```

### TUI keybindings

| Key       | Action                          |
|-----------|---------------------------------|
| `j` / `k` | Move down / up                  |
| `Enter`   | Select category / play station  |
| `b`       | Back out to category list       |
| `s`       | Stop playback                   |
| `p`       | Pause / resume                  |
| `+` / `-` | Volume up / down                |
| `q`       | Quit                            |

## Adding stations

Stations live in the `STATIONS` const slice at the top of `src/main.rs`. Append a
`Station { name, url, category, desc, quality }` entry and rebuild — categories
are derived automatically from the `category` field in insertion order.
