# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`radio` is an interactive terminal radio streaming utility for macOS. The current implementation is a Rust binary (`src/main.rs`); the original bash version (`./radio`) is preserved for reference but no longer the primary path.

## Building & running

- `cargo build --release` — produces `target/release/radio` (~640 KB, single binary).
- `cargo run --release` — build + run TUI.
- `cargo run --release -- --list` / `--help` / `<station-name>`

Runtime requirement: `ffmpeg` on `PATH`. Verified at startup with a `which ffmpeg` check.

## Why the rewrite

The bash version segfaulted bash 3.2 every ~10 minutes when running with any meaningful render rate. Crash signature was always SIGSEGV at `0xdfdfdfdfdfdfdfe7` (macOS arm64 freed-memory pattern) inside `__kill`, with deep trap-handler recursion — async signals interrupting bash's malloc critical sections corrupt the heap. We tried subshell SIGALRM timer (segfault), TIOCSTI byte injection (audio stutter), perl-driven kill ALRM at 30 Hz (segfault), data-driven kill ALRM at 2 Hz (still segfaulted, just slower). No bash 3.2 design survived a long soak.

In Rust we have a real event loop (`std::sync::mpsc::channel`) and proper threads, so the whole signal-shenanigans story disappears.

## Architecture

Single binary, three threads:

1. **Main**: render loop. Blocks on `rx.recv_timeout(1s)`. Drains all queued events before each render so bursty waveform updates produce one redraw, not N.
2. **Input thread**: `event::read()` from `crossterm`, forwards `AppEvent::Input` to main.
3. **Waveform thread** (one per active stream): owns ffmpeg's stdout pipe, parses 8 kHz mono u8 PCM into 120-sample columns, encodes to Braille bytes, sends `AppEvent::Wave { top, bot }` to main. Coalesces to ≤60 Hz via `Instant::now()` check so a producer that delivers in tiny pieces doesn't spam the channel.

### Audio pipeline

`launch_stream(url)` spawns one ffmpeg subprocess with **two outputs from one network read**:
1. `-f audiotoolbox -` → system audio.
2. `-ac 1 -ar 8000 -f u8 pipe:1` → stdout, captured by the waveform thread.

`std::process::Child::id()` gives us ffmpeg's PID directly, so pause/resume is just `libc::kill(pid, SIGSTOP|SIGCONT)`.

`AudioProcess::stop()` is the canonical teardown: signals the waveform thread to stop, sends SIGCONT (in case the process was paused — SIGTERM on a stopped process leaves a zombie), kills the child, joins the thread.

### Auto-reconnect

After each event batch, `App::check_playback()` calls `child.try_wait()`. If ffmpeg died and we're under `MAX_RECONNECT=3`, it tears the dead process down and relaunches the same URL.

### Navigation model

Two-level: category → stations within category. `category_cursor` selects the category; `station_cursor = -1` means the cursor is on the category header itself, `>= 0` indexes into `expanded` (the station indices belonging to the currently expanded category). Only one category is expanded at a time. `expand_current_category()` repopulates `expanded` whenever `category_cursor` changes.

### Rendering

Direct `crossterm::queue!` macros into a buffered stdout, flushed once per render. The whole frame is re-emitted every render (no diffing) — at most ~60 Hz, into a single `BufWriter`, this is cheap. Layout matches the bash version for keybinding parity (j/k nav, enter select, b back, s stop, p pause, +/- volume, q quit). `WAVE_COL = 48` is the column where the waveform meter starts.

The waveform meter is two rows of Braille glyphs (`U+2800 + b`, three UTF-8 bytes per glyph). Each glyph encodes the peak amplitude of two sample halves — see `TLF`/`TRF`/`BLF`/`BRF` tables. They're load-bearing constants ported from the original bash/perl; don't change them without changing both halves of the encoding.

### Volume

macOS-only via `osascript`. `get_volume()` parses `output volume of (get volume settings)`; `adjust_volume(delta)` clamps and fires `set volume output volume N` in a detached thread (osascript can take 50–100 ms and we don't want to stall the event loop).

## Cleanup invariants

`TerminalGuard` (RAII) handles alt-screen exit, cursor restore, raw-mode disable on every drop path including panics (`install_panic_handler` chains a restore step). `App::stop_playback` is called explicitly at the end of `run_tui` to make sure ffmpeg/waveform-thread don't outlive the TUI.

## Adding stations

Append to the `STATIONS` const slice. Categories group automatically — `App::new()` derives the category list and counts in insertion order.

## The legacy bash version

`./radio` (the bash script) is still in the repo. It works, but will eventually segfault (see "Why the rewrite"). Keep it around for reference; don't extend it.
