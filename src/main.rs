use std::io::{self, Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crossterm::{
    cursor::{Hide, MoveTo, Show},
    event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
    queue,
    style::{Attribute, Color, Print, ResetColor, SetAttribute, SetForegroundColor},
    terminal::{
        self, Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen,
    },
    ExecutableCommand,
};

// ---------------------------------------------------------------------------
// Station database
// ---------------------------------------------------------------------------
struct Station {
    name: &'static str,
    url: &'static str,
    category: &'static str,
    desc: &'static str,
    quality: &'static str,
}

const STATIONS: &[Station] = &[
    Station {
        name: "BBC 6 Music",
        url: "https://as-hls-ww.live.cf.md.bbci.co.uk/pool_81827798/live/ww/bbc_6music/bbc_6music.isml/bbc_6music-audio=320000.norewind.m3u8",
        category: "BBC",
        desc: "Alternative music station",
        quality: "AAC 320k",
    },
    Station { name: "FIP", url: "https://icecast.radiofrance.fr/fip-hifi.aac", category: "FIP", desc: "Eclectic French public radio", quality: "AAC Hi-Fi" },
    Station { name: "FIP Rock", url: "https://icecast.radiofrance.fr/fiprock-hifi.aac", category: "FIP", desc: "Rock from all eras", quality: "AAC Hi-Fi" },
    Station { name: "FIP Jazz", url: "https://icecast.radiofrance.fr/fipjazz-hifi.aac", category: "FIP", desc: "Jazz standards and discoveries", quality: "AAC Hi-Fi" },
    Station { name: "FIP Groove", url: "https://icecast.radiofrance.fr/fipgroove-hifi.aac", category: "FIP", desc: "Funk, soul, and groove", quality: "AAC Hi-Fi" },
    Station { name: "FIP Pop", url: "https://icecast.radiofrance.fr/fippop-hifi.aac", category: "FIP", desc: "Pop music selections", quality: "AAC Hi-Fi" },
    Station { name: "FIP Electro", url: "https://icecast.radiofrance.fr/fipelectro-hifi.aac", category: "FIP", desc: "Electronic music", quality: "AAC Hi-Fi" },
    Station { name: "FIP Monde", url: "https://icecast.radiofrance.fr/fipmonde-hifi.aac", category: "FIP", desc: "World music", quality: "AAC Hi-Fi" },
    Station { name: "FIP Reggae", url: "https://icecast.radiofrance.fr/fipreggae-hifi.aac", category: "FIP", desc: "Reggae and dub", quality: "AAC Hi-Fi" },
    Station { name: "FIP Nouveautes", url: "https://icecast.radiofrance.fr/fipnouveautes-hifi.aac", category: "FIP", desc: "New releases", quality: "AAC Hi-Fi" },
    Station { name: "FIP Metal", url: "https://icecast.radiofrance.fr/fipmetal-hifi.aac", category: "FIP", desc: "Metal and heavy music", quality: "AAC Hi-Fi" },
    Station { name: "France Musique", url: "https://icecast.radiofrance.fr/francemusique-hifi.aac", category: "France Musique", desc: "Classical and jazz", quality: "AAC Hi-Fi" },
    Station { name: "KALX", url: "https://stream.kalx.berkeley.edu:8443/kalx-128.mp3", category: "KALX", desc: "UC Berkeley college radio", quality: "MP3 128k" },
    Station { name: "KCRW Eclectic 24", url: "https://streams.kcrw.com/e24_aac", category: "KCRW", desc: "24/7 hand-picked music", quality: "AAC" },
    Station { name: "KCRW Live", url: "https://streams.kcrw.com/kcrw_aac", category: "KCRW", desc: "Live simulcast", quality: "AAC" },
    Station { name: "KEXP", url: "https://kexp.streamguys1.com/kexp160.aac", category: "KEXP", desc: "Seattle's independent radio", quality: "AAC 160k" },
    Station { name: "NTS 1", url: "https://stream-relay-geo.ntslive.net/stream", category: "NTS", desc: "London-based internet radio, channel 1", quality: "MP3 192k" },
    Station { name: "NTS 2", url: "https://stream-relay-geo.ntslive.net/stream2", category: "NTS", desc: "London-based internet radio, channel 2", quality: "MP3 192k" },
    Station { name: "WFMU", url: "http://stream0.wfmu.org/freeform-128k", category: "WFMU", desc: "Freeform radio from Jersey City", quality: "MP3 128k" },
];

// ---------------------------------------------------------------------------
// Constants & types
// ---------------------------------------------------------------------------
const MAX_RECONNECT: u8 = 3;
const SAMPLES_PER_COL: usize = 120;
const WAVE_BUFFER_COLS: usize = 200;
const WAVE_COL: u16 = 48;

// Braille bit-pattern tables (level 0..4 → bits to set per row/column).
// These match the original bash/perl implementation byte-for-byte; the
// braille char for byte b is U+2800+b, which encodes as the three bytes
// (0xE2, 0xA0 + b/64, 0x80 + b%64) in UTF-8.
const TLF: [u8; 5] = [0, 0x40, 0x44, 0x46, 0x47];
const TRF: [u8; 5] = [0, 0x80, 0xA0, 0xB0, 0xB8];
const BLF: [u8; 5] = [0, 0x01, 0x03, 0x07, 0x47];
const BRF: [u8; 5] = [0, 0x08, 0x18, 0x38, 0xB8];

fn braille_char(b: u8) -> [u8; 3] {
    [0xE2, 0xA0 + (b / 64), 0x80 + (b % 64)]
}

enum AppEvent {
    Input(Event),
    Wave { top: String, bot: String },
    Metadata(Option<String>),
    StreamDied,
    Tick,
}

struct AudioProcess {
    child: Child,
    pid: u32,
    stop_flag: Arc<AtomicBool>,
    wave_thread: Option<thread::JoinHandle<()>>,
    meta_thread: Option<thread::JoinHandle<()>>,
}

impl AudioProcess {
    fn stop(mut self) {
        self.stop_flag.store(true, Ordering::Relaxed);
        // Resume the process if it was paused; SIGTERM on a stopped process
        // would otherwise leave a zombie until SIGCONT.
        unsafe {
            libc::kill(self.pid as i32, libc::SIGCONT);
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(h) = self.wave_thread.take() {
            let _ = h.join();
        }
        // Don't join the metadata thread: it may be parked in an HTTP read
        // with up to a 30s timeout. Drop the handle to detach instead. The
        // thread observes stop_flag between reads and exits on its own.
        drop(self.meta_thread.take());
    }
}

struct App {
    categories: Vec<&'static str>,
    cat_counts: Vec<usize>,
    category_cursor: usize,
    station_cursor: i32,
    expanded: Vec<usize>,
    current_idx: Option<usize>,
    paused: bool,
    audio: Option<AudioProcess>,
    reconnect_attempts: u8,
    wave_top: String,
    wave_bot: String,
    metadata: Option<String>,
    volume: Option<u8>,
    term_size: (u16, u16),
    event_tx: Sender<AppEvent>,
}

impl App {
    fn new(event_tx: Sender<AppEvent>) -> Self {
        let mut categories: Vec<&'static str> = Vec::new();
        let mut cat_counts: Vec<usize> = Vec::new();
        for s in STATIONS {
            if !categories.contains(&s.category) {
                categories.push(s.category);
                let c = STATIONS.iter().filter(|x| x.category == s.category).count();
                cat_counts.push(c);
            }
        }
        let term_size = terminal::size().unwrap_or((80, 24));
        let mut app = App {
            categories,
            cat_counts,
            category_cursor: 0,
            station_cursor: -1,
            expanded: Vec::new(),
            current_idx: None,
            paused: false,
            audio: None,
            reconnect_attempts: 0,
            wave_top: String::new(),
            wave_bot: String::new(),
            metadata: None,
            volume: get_volume(),
            term_size,
            event_tx,
        };
        app.expand_current_category();
        app
    }

    fn expand_current_category(&mut self) {
        let cat = self.categories[self.category_cursor];
        self.expanded = STATIONS
            .iter()
            .enumerate()
            .filter(|(_, s)| s.category == cat)
            .map(|(i, _)| i)
            .collect();
    }

    fn vu_width(&self) -> u16 {
        if self.term_size.0 < 55 {
            return 0;
        }
        let w = self.term_size.0.saturating_sub(WAVE_COL);
        w.clamp(5, 200)
    }

    fn truncate_wave(&self, s: &str) -> String {
        // Each braille glyph is 3 UTF-8 bytes wide and 1 column visually.
        let vu = self.vu_width() as usize;
        if vu == 0 || s.is_empty() {
            return String::new();
        }
        let glyph_count = s.len() / 3;
        if glyph_count <= vu {
            return s.to_string();
        }
        let skip_bytes = (glyph_count - vu) * 3;
        s[skip_bytes..].to_string()
    }

    fn handle_key(&mut self, key: KeyEvent) -> bool {
        if key.kind != KeyEventKind::Press && key.kind != KeyEventKind::Repeat {
            return true;
        }
        let max_cat = self.categories.len() as i32 - 1;
        let max_station = self.expanded.len() as i32 - 1;
        match (key.code, key.modifiers) {
            (KeyCode::Char('q'), _) => return false,
            (KeyCode::Char('c'), KeyModifiers::CONTROL) => return false,
            (KeyCode::Up, _) | (KeyCode::Char('k'), _) => {
                if self.station_cursor > 0 {
                    self.station_cursor -= 1;
                } else if self.station_cursor == 0 {
                    self.station_cursor = -1;
                } else if self.category_cursor > 0 {
                    self.category_cursor -= 1;
                    self.expand_current_category();
                }
            }
            (KeyCode::Down, _) | (KeyCode::Char('j'), _) => {
                if self.station_cursor >= 0 {
                    if self.station_cursor < max_station {
                        self.station_cursor += 1;
                    } else {
                        self.station_cursor = -1;
                        if (self.category_cursor as i32) < max_cat {
                            self.category_cursor += 1;
                            self.expand_current_category();
                        } else {
                            self.station_cursor = max_station;
                        }
                    }
                } else if (self.category_cursor as i32) < max_cat {
                    self.category_cursor += 1;
                    self.expand_current_category();
                }
            }
            (KeyCode::Tab, _) => {
                if self.station_cursor == -1 {
                    self.station_cursor = 0;
                } else if self.station_cursor < max_station {
                    self.station_cursor += 1;
                } else {
                    self.station_cursor = -1;
                }
            }
            (KeyCode::Enter, _) => {
                let idx = if self.station_cursor == -1 {
                    self.expanded.first().copied()
                } else {
                    self.expanded.get(self.station_cursor as usize).copied()
                };
                if let Some(i) = idx {
                    self.start_playback(i);
                }
            }
            (KeyCode::Char('b'), _) | (KeyCode::BackTab, _) => {
                if self.station_cursor >= 0 {
                    self.station_cursor = -1;
                }
            }
            (KeyCode::Char('s'), _) => {
                self.stop_playback();
                self.current_idx = None;
            }
            (KeyCode::Char('p'), _) => {
                self.toggle_pause();
            }
            (KeyCode::Char('+'), _) | (KeyCode::Char('='), _) => {
                self.adjust_volume(5);
            }
            (KeyCode::Char('-'), _) => {
                self.adjust_volume(-5);
            }
            _ => {}
        }
        true
    }

    fn start_playback(&mut self, idx: usize) {
        if self.current_idx == Some(idx) {
            if let Some(a) = &mut self.audio {
                if a.child.try_wait().ok().flatten().is_none() && !self.paused {
                    return;
                }
            }
        }
        self.stop_playback();
        match launch_stream(STATIONS[idx].url, self.event_tx.clone()) {
            Ok(audio) => {
                self.audio = Some(audio);
                self.current_idx = Some(idx);
                self.reconnect_attempts = 0;
                self.paused = false;
            }
            Err(e) => {
                eprintln!("radio: failed to start: {}", e);
            }
        }
    }

    fn stop_playback(&mut self) {
        if let Some(a) = self.audio.take() {
            a.stop();
        }
        self.paused = false;
        self.wave_top.clear();
        self.wave_bot.clear();
        self.metadata = None;
        self.reconnect_attempts = 0;
    }

    fn toggle_pause(&mut self) {
        let Some(a) = &self.audio else { return };
        let sig = if self.paused { libc::SIGCONT } else { libc::SIGSTOP };
        unsafe {
            libc::kill(a.pid as i32, sig);
        }
        self.paused = !self.paused;
    }

    fn adjust_volume(&mut self, delta: i32) {
        let cur = self.volume.unwrap_or(50) as i32;
        let new_vol = (cur + delta).clamp(0, 100) as u8;
        // Fire-and-forget: AppleScript can take 50–100ms and we don't want
        // to block the event loop on it.
        thread::spawn(move || {
            let _ = Command::new("osascript")
                .args(["-e", &format!("set volume output volume {}", new_vol)])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        });
        self.volume = Some(new_vol);
    }

    fn check_playback(&mut self) {
        let Some(audio) = &mut self.audio else { return };
        let alive = matches!(audio.child.try_wait(), Ok(None));
        if alive {
            return;
        }
        let Some(idx) = self.current_idx else {
            self.audio = None;
            return;
        };
        if self.reconnect_attempts >= MAX_RECONNECT {
            self.stop_playback();
            self.current_idx = None;
            return;
        }
        // Stream died; tear down the dead one and start a fresh one.
        let dead = self.audio.take().unwrap();
        dead.stop();
        self.reconnect_attempts += 1;
        match launch_stream(STATIONS[idx].url, self.event_tx.clone()) {
            Ok(a) => self.audio = Some(a),
            Err(_) => {
                self.current_idx = None;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Volume (macOS osascript)
// ---------------------------------------------------------------------------
fn get_volume() -> Option<u8> {
    let out = Command::new("osascript")
        .args(["-e", "output volume of (get volume settings)"])
        .stderr(Stdio::null())
        .output()
        .ok()?;
    let s = String::from_utf8_lossy(&out.stdout);
    s.trim().parse::<u8>().ok()
}

// ---------------------------------------------------------------------------
// Audio + waveform pipeline
// ---------------------------------------------------------------------------
fn launch_stream(url: &str, tx: Sender<AppEvent>) -> io::Result<AudioProcess> {
    let mut child = Command::new("ffmpeg")
        .args([
            "-loglevel", "quiet",
            "-nostdin",
            "-i", url,
            "-f", "audiotoolbox", "-",
            "-ac", "1",
            "-ar", "8000",
            "-f", "u8",
            "pipe:1",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let pid = child.id();
    let stdout = child.stdout.take().unwrap();
    let stop = Arc::new(AtomicBool::new(false));

    let wave_tx = tx.clone();
    let wave_stop = stop.clone();
    let wave_handle = thread::spawn(move || waveform_loop(stdout, wave_tx, wave_stop));

    // ICY metadata: open a parallel HTTP connection with `Icy-MetaData: 1`
    // and parse the metadata bursts that the icecast/shoutcast server
    // interleaves into the stream. ffmpeg parses ICY internally but does
    // not log dynamic track changes — only the metadata at stream-open —
    // so we need our own reader to surface live track titles. HLS streams
    // (e.g. BBC) and any server that doesn't return `icy-metaint` will be
    // skipped by `metadata_fetch_loop` and just show no track info.
    let url_owned = url.to_string();
    let meta_tx = tx.clone();
    let meta_stop = stop.clone();
    let meta_handle = thread::spawn(move || metadata_fetch_loop(url_owned, meta_tx, meta_stop));

    Ok(AudioProcess {
        child,
        pid,
        stop_flag: stop,
        wave_thread: Some(wave_handle),
        meta_thread: Some(meta_handle),
    })
}

fn metadata_fetch_loop(url: String, tx: Sender<AppEvent>, stop: Arc<AtomicBool>) {
    let mut backoff_secs: u64 = 1;
    while !stop.load(Ordering::Relaxed) {
        match fetch_icy_once(&url, &tx, &stop) {
            Ok(true) => {
                // Stream told us it has no ICY support; don't reconnect.
                return;
            }
            Ok(false) => {
                // Connection ended cleanly (EOF). Reconnect on a tight
                // backoff in case it's a transient hiccup.
                backoff_secs = 1;
            }
            Err(_) => {
                backoff_secs = (backoff_secs * 2).min(30);
            }
        }
        for _ in 0..(backoff_secs * 10) {
            if stop.load(Ordering::Relaxed) { return; }
            thread::sleep(Duration::from_millis(100));
        }
    }
}

// Returns Ok(true) if the server has no ICY metadata support (skip retry),
// Ok(false) for clean EOF, Err for a connection problem worth retrying.
fn fetch_icy_once(url: &str, tx: &Sender<AppEvent>, stop: &Arc<AtomicBool>) -> io::Result<bool> {
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(10))
        // Per-read timeout. If the server stops sending we error out and
        // retry, also breaking out of an in-progress read when the user
        // stops playback (stop_flag is checked between reads).
        .timeout_read(Duration::from_secs(30))
        .user_agent("radio/0.1")
        .build();

    let resp = agent
        .get(url)
        .set("Icy-MetaData", "1")
        .call()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

    let metaint: usize = match resp.header("icy-metaint") {
        Some(s) => match s.parse() {
            Ok(n) if n > 0 => n,
            _ => return Ok(true),
        },
        None => return Ok(true),
    };

    let mut reader = resp.into_reader();
    let mut audio_buf = vec![0u8; metaint];
    let mut last_sent: Option<String> = None;

    loop {
        if stop.load(Ordering::Relaxed) {
            return Ok(false);
        }
        if let Err(e) = reader.read_exact(&mut audio_buf) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return Ok(false);
            }
            return Err(e);
        }
        let mut len_byte = [0u8; 1];
        if let Err(e) = reader.read_exact(&mut len_byte) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return Ok(false);
            }
            return Err(e);
        }
        let meta_len = (len_byte[0] as usize) * 16;
        if meta_len == 0 {
            continue;
        }
        let mut meta_buf = vec![0u8; meta_len];
        if let Err(e) = reader.read_exact(&mut meta_buf) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return Ok(false);
            }
            return Err(e);
        }
        // Servers null-pad metadata up to the next 16-byte boundary.
        let end = meta_buf.iter().position(|&b| b == 0).unwrap_or(meta_buf.len());
        let meta_str = String::from_utf8_lossy(&meta_buf[..end]);
        let title = parse_icy_title(&meta_str).map(|t| t.trim().to_string());
        let title = title.filter(|t| t.chars().any(|c| c.is_alphanumeric()));
        if title != last_sent {
            if tx.send(AppEvent::Metadata(title.clone())).is_err() {
                return Ok(false);
            }
            last_sent = title;
        }
    }
}

// ICY metadata surfaces in ffmpeg stderr as either:
//   StreamTitle='Artist - Track';StreamUrl='...';   (raw payload form)
//   "    StreamTitle     : Artist - Track"          (metadata block form,
//                                                    variable padding)
fn parse_icy_title(line: &str) -> Option<&str> {
    if let Some(idx) = line.find("StreamTitle='") {
        let after = &line[idx + "StreamTitle='".len()..];
        if let Some(end) = after.find("';") {
            return Some(&after[..end]);
        }
        if let Some(end) = after.rfind('\'') {
            return Some(&after[..end]);
        }
    }
    if let Some(idx) = line.find("StreamTitle") {
        let rest = &line[idx + "StreamTitle".len()..];
        let rest = rest.trim_start();
        if let Some(colon_pos) = rest.find(':') {
            return Some(rest[colon_pos + 1..].trim());
        }
    }
    None
}

fn waveform_loop(mut stdout: impl Read, tx: Sender<AppEvent>, stop: Arc<AtomicBool>) {
    let mut buf = [0u8; 4096];
    let mut samples: Vec<u8> = Vec::with_capacity(8192);
    let mut top_buf: Vec<u8> = Vec::with_capacity(WAVE_BUFFER_COLS * 3);
    let mut bot_buf: Vec<u8> = Vec::with_capacity(WAVE_BUFFER_COLS * 3);
    let mut last_send = Instant::now() - Duration::from_secs(1);

    while !stop.load(Ordering::Relaxed) {
        let n = match stdout.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => break,
        };
        samples.extend_from_slice(&buf[..n]);

        let mut changed = false;
        while samples.len() >= SAMPLES_PER_COL {
            let half = SAMPLES_PER_COL / 2;
            let mut pl: u8 = 0;
            let mut pr: u8 = 0;
            for i in 0..half {
                let a = (samples[i] as i16 - 128).unsigned_abs() as u8;
                if a > pl { pl = a; }
            }
            for i in half..SAMPLES_PER_COL {
                let a = (samples[i] as i16 - 128).unsigned_abs() as u8;
                if a > pr { pr = a; }
            }
            samples.drain(..SAMPLES_PER_COL);

            let ll = ((pl as u32 * 4) / 80).min(4) as usize;
            let rl = ((pr as u32 * 4) / 80).min(4) as usize;
            let to = TLF[ll] | TRF[rl];
            let bo = BLF[ll] | BRF[rl];

            top_buf.extend_from_slice(&braille_char(to));
            bot_buf.extend_from_slice(&braille_char(bo));
            let max_bytes = WAVE_BUFFER_COLS * 3;
            if top_buf.len() > max_bytes {
                let drop = top_buf.len() - max_bytes;
                top_buf.drain(..drop);
            }
            if bot_buf.len() > max_bytes {
                let drop = bot_buf.len() - max_bytes;
                bot_buf.drain(..drop);
            }
            changed = true;
        }

        // Coalesce: send updates at most ~60 Hz to keep render churn low even
        // if the audio side delivers in tiny pieces.
        if changed && last_send.elapsed() >= Duration::from_millis(16) {
            let top = unsafe { String::from_utf8_unchecked(top_buf.clone()) };
            let bot = unsafe { String::from_utf8_unchecked(bot_buf.clone()) };
            if tx.send(AppEvent::Wave { top, bot }).is_err() {
                break;
            }
            last_send = Instant::now();
        }
    }
    let _ = tx.send(AppEvent::StreamDied);
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------
fn render(app: &App, out: &mut impl Write) -> io::Result<()> {
    let (cols, rows) = app.term_size;
    queue!(out, MoveTo(0, 0))?;
    render_header(app, out)?;

    let mut row: u16 = 2;
    for (cat_idx, cat) in app.categories.iter().enumerate() {
        let count = app.cat_counts[cat_idx];
        let is_current_cat = cat_idx == app.category_cursor;
        let marker = if is_current_cat { "▾" } else { "▸" };
        if row >= rows.saturating_sub(1) { break; }

        queue!(out, MoveTo(0, row))?;
        if is_current_cat && app.station_cursor == -1 {
            queue!(out,
                Print("  "),
                SetForegroundColor(Color::Cyan),
                Print(">"),
                ResetColor,
                Print(" "),
                SetAttribute(Attribute::Bold),
                SetForegroundColor(Color::White),
                Print(format!("{} {:<28}", marker, cat)),
                ResetColor,
                Print("  "),
                SetAttribute(Attribute::Dim),
                Print(format!("{}", count)),
                ResetColor,
            )?;
        } else {
            queue!(out,
                Print(format!("   {} {:<28}  ", marker, cat)),
                SetAttribute(Attribute::Dim),
                Print(format!("{}", count)),
                ResetColor,
            )?;
        }
        queue!(out, Clear(ClearType::UntilNewLine))?;
        row += 1;

        if !is_current_cat {
            continue;
        }

        for (s_idx, &station_idx) in app.expanded.iter().enumerate() {
            if row >= rows.saturating_sub(1) { break; }
            let s = &STATIONS[station_idx];
            let is_playing = app.current_idx == Some(station_idx);
            let cursor_here = s_idx as i32 == app.station_cursor;

            queue!(out, MoveTo(0, row))?;
            if cursor_here {
                queue!(out,
                    Print("    "),
                    SetForegroundColor(Color::Cyan),
                    Print(">"),
                    ResetColor,
                    Print(" "),
                    SetAttribute(Attribute::Bold),
                    SetForegroundColor(Color::White),
                    Print(format!("{:<30}", s.name)),
                    ResetColor,
                    Print(" "),
                    SetForegroundColor(Color::Yellow),
                    Print(format!("{:<7}", s.quality)),
                    ResetColor,
                )?;
            } else {
                queue!(out,
                    Print(format!("     {:<30} ", s.name)),
                    SetAttribute(Attribute::Dim),
                    Print(format!("{:<7}", s.quality)),
                    ResetColor,
                )?;
            }
            if is_playing {
                if app.paused {
                    queue!(out, SetForegroundColor(Color::Yellow), Print(" ▐▐"), ResetColor)?;
                } else {
                    queue!(out, SetForegroundColor(Color::Green), Print(" ▶"), ResetColor)?;
                }
            }
            queue!(out, Clear(ClearType::UntilNewLine))?;

            // Waveform top row
            if is_playing && !app.wave_top.is_empty() {
                queue!(out,
                    MoveTo(WAVE_COL.saturating_sub(1), row),
                    SetForegroundColor(Color::Green),
                    Print(app.truncate_wave(&app.wave_top)),
                    ResetColor,
                )?;
            }

            row += 1;
            if row >= rows.saturating_sub(1) { break; }

            // Description row (and waveform bottom row)
            queue!(out, MoveTo(0, row))?;
            if cursor_here {
                queue!(out,
                    Print("      "),
                    SetAttribute(Attribute::Dim),
                    Print(s.desc),
                    ResetColor,
                )?;
                queue!(out, Clear(ClearType::UntilNewLine))?;
            } else if is_playing {
                // Reserve an empty row so layout doesn't jitter when the
                // waveform writer hasn't produced a bottom row yet.
                queue!(out, Clear(ClearType::UntilNewLine))?;
            }
            if is_playing && !app.wave_bot.is_empty() {
                queue!(out,
                    MoveTo(WAVE_COL.saturating_sub(1), row),
                    SetForegroundColor(Color::Green),
                    Print(app.truncate_wave(&app.wave_bot)),
                    ResetColor,
                )?;
            }
            if cursor_here || is_playing {
                row += 1;
            }
        }
    }

    // Erase remaining rows
    while row < rows.saturating_sub(1) {
        queue!(out, MoveTo(0, row), Clear(ClearType::UntilNewLine))?;
        row += 1;
    }

    render_footer(app, out)?;
    out.flush()?;
    let _ = cols;
    Ok(())
}

fn render_header(app: &App, out: &mut impl Write) -> io::Result<()> {
    let width = app.term_size.0;
    if width == 0 {
        return Ok(());
    }
    let title = " RADIO";
    let vol_info = match app.volume {
        Some(v) => format!("  Vol: {}", v),
        None => String::new(),
    };
    let left = format!("{}{}", title, vol_info);

    let now_playing = match app.current_idx {
        Some(idx) => {
            let pause_marker = if app.paused { " [paused]" } else { "" };
            let title_part = match &app.metadata {
                Some(t) => format!(" — {}", t),
                None => String::new(),
            };
            format!("  {}{}{} ", STATIONS[idx].name, title_part, pause_marker)
        }
        None => String::new(),
    };

    // Paint the row as a reverse-bold fill, then overlay the two pieces at
    // their columns. Computed positioning avoids any chance of the right
    // half wrapping off-screen due to width miscounts.
    queue!(
        out,
        MoveTo(0, 0),
        SetAttribute(Attribute::Reverse),
        SetAttribute(Attribute::Bold),
        Print(" ".repeat(width as usize)),
        MoveTo(0, 0),
        Print(&left),
    )?;

    if !now_playing.is_empty() {
        let right_len = now_playing.chars().count() as u16;
        let left_len = left.chars().count() as u16;
        if right_len < width && width.saturating_sub(right_len) > left_len {
            let col = width - right_len;
            queue!(
                out,
                MoveTo(col, 0),
                SetForegroundColor(Color::Green),
                Print(&now_playing),
            )?;
        }
    }
    queue!(out, SetAttribute(Attribute::Reset), ResetColor)?;
    Ok(())
}

fn render_footer(app: &App, out: &mut impl Write) -> io::Result<()> {
    let width = app.term_size.0 as usize;
    let last_row = app.term_size.1.saturating_sub(1);
    let hints = " j/k:nav  enter:select  b:back  s:stop  p:pause  +/-:vol  q:quit";
    let pad = width.saturating_sub(hints.chars().count());
    queue!(out,
        MoveTo(0, last_row),
        SetAttribute(Attribute::Reverse),
        SetAttribute(Attribute::Dim),
        Print(hints),
        Print(" ".repeat(pad)),
        ResetColor,
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Terminal lifecycle
// ---------------------------------------------------------------------------
struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> io::Result<Self> {
        terminal::enable_raw_mode()?;
        io::stdout().execute(EnterAlternateScreen)?.execute(Hide)?;
        Ok(TerminalGuard)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = io::stdout().execute(Show);
        let _ = io::stdout().execute(LeaveAlternateScreen);
        let _ = terminal::disable_raw_mode();
    }
}

fn install_panic_handler() {
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = io::stdout().execute(Show);
        let _ = io::stdout().execute(LeaveAlternateScreen);
        let _ = terminal::disable_raw_mode();
        default(info);
    }));
}

// ---------------------------------------------------------------------------
// CLI / entrypoints
// ---------------------------------------------------------------------------
fn print_help() {
    println!("Usage: radio [station-name | --list | --help]");
    println!();
    println!("Interactive terminal radio player.");
    println!();
    println!("Options:");
    println!("  (no args)       Launch interactive TUI");
    println!("  <station-name>  Play station directly (fuzzy match)");
    println!("  --list, -l      List all available stations");
    println!("  --help, -h      Show this help message");
}

fn print_station_list() {
    let mut current_category = "";
    let is_tty = std::io::IsTerminal::is_terminal(&io::stdout());
    let bold = if is_tty { "\x1b[1m" } else { "" };
    let dim = if is_tty { "\x1b[2m" } else { "" };
    let reset = if is_tty { "\x1b[0m" } else { "" };
    for s in STATIONS {
        if s.category != current_category {
            current_category = s.category;
            println!();
            println!("{}{}{}", bold, current_category, reset);
        }
        println!(
            "  {:<36} {}{:<10}{}  {}",
            s.name, dim, s.quality, reset, s.desc
        );
    }
    println!();
}

fn fuzzy_match(query: &str) -> Option<usize> {
    let q = query.to_lowercase();
    for (i, s) in STATIONS.iter().enumerate() {
        if s.name.to_lowercase() == q {
            return Some(i);
        }
    }
    for (i, s) in STATIONS.iter().enumerate() {
        if s.name.to_lowercase().contains(&q) {
            return Some(i);
        }
    }
    None
}

fn direct_play(idx: usize) -> io::Result<i32> {
    let s = &STATIONS[idx];
    let is_tty = std::io::IsTerminal::is_terminal(&io::stdout());
    let bold = if is_tty { "\x1b[1m" } else { "" };
    let dim = if is_tty { "\x1b[2m" } else { "" };
    let reset = if is_tty { "\x1b[0m" } else { "" };
    println!("{}Playing:{} {}  {}[{}]{}", bold, reset, s.name, dim, s.quality, reset);
    println!("{}Press Ctrl+C to stop{}", dim, reset);
    let status = Command::new("ffmpeg")
        .args([
            "-loglevel", "error",
            "-nostdin",
            "-i", s.url,
            "-f", "audiotoolbox", "-",
        ])
        .status()?;
    Ok(status.code().unwrap_or(0))
}

fn require_command(cmd: &str) -> io::Result<()> {
    let status = Command::new("which")
        .arg(cmd)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    if !status.success() {
        eprintln!("radio: {} not found. Install ffmpeg first.", cmd);
        eprintln!("  brew install ffmpeg");
        std::process::exit(1);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// TUI main loop
// ---------------------------------------------------------------------------
fn run_tui() -> io::Result<()> {
    require_command("ffmpeg")?;
    install_panic_handler();
    let _guard = TerminalGuard::enter()?;

    let (tx, rx): (Sender<AppEvent>, Receiver<AppEvent>) = mpsc::channel();

    // Input reader thread: blocks on event::read() and forwards to channel.
    let input_tx = tx.clone();
    thread::spawn(move || {
        loop {
            match event::read() {
                Ok(ev) => {
                    if input_tx.send(AppEvent::Input(ev)).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    let mut app = App::new(tx.clone());
    let mut stdout = io::BufWriter::new(io::stdout());
    queue!(stdout, Clear(ClearType::All))?;
    render(&app, &mut stdout)?;

    // The render path is allowed to coalesce multiple Wave/Tick events:
    // we drain the channel after each blocking recv so a burst of updates
    // produces just one redraw.
    loop {
        let ev = match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(e) => e,
            Err(RecvTimeoutError::Timeout) => AppEvent::Tick,
            Err(RecvTimeoutError::Disconnected) => break,
        };
        let mut events = vec![ev];
        while let Ok(e) = rx.try_recv() {
            events.push(e);
        }

        let mut should_quit = false;
        let mut needs_full_clear = false;
        for ev in events {
            match ev {
                AppEvent::Input(Event::Key(k)) => {
                    if !app.handle_key(k) {
                        should_quit = true;
                    }
                }
                AppEvent::Input(Event::Resize(c, r)) => {
                    app.term_size = (c, r);
                    needs_full_clear = true;
                }
                AppEvent::Input(_) => {}
                AppEvent::Wave { top, bot } => {
                    app.wave_top = top;
                    app.wave_bot = bot;
                }
                AppEvent::Metadata(title) => {
                    app.metadata = title;
                }
                AppEvent::StreamDied => {
                    // check_playback below will handle reconnect logic
                }
                AppEvent::Tick => {}
            }
        }
        if should_quit {
            break;
        }
        app.check_playback();
        if needs_full_clear {
            queue!(stdout, Clear(ClearType::All))?;
        }
        render(&app, &mut stdout)?;
    }

    app.stop_playback();
    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        if let Err(e) = run_tui() {
            eprintln!("radio: {}", e);
            std::process::exit(1);
        }
        println!("radio: goodbye");
        return;
    }
    match args[0].as_str() {
        "--help" | "-h" => {
            print_help();
        }
        "--list" | "-l" => {
            print_station_list();
        }
        _ => {
            if let Err(e) = require_command("ffmpeg") {
                eprintln!("radio: {}", e);
                std::process::exit(1);
            }
            let query = args.join(" ");
            match fuzzy_match(&query) {
                Some(i) => match direct_play(i) {
                    Ok(0) => {}
                    Ok(rc) => std::process::exit(rc),
                    Err(e) => {
                        eprintln!("radio: ffmpeg failed: {}", e);
                        std::process::exit(1);
                    }
                },
                None => {
                    eprintln!("radio: no station matching \"{}\"", query);
                    eprintln!("Try: radio --list");
                    std::process::exit(1);
                }
            }
        }
    }
}
