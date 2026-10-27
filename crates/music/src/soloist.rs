//! Spotify's official audio engine, controlled locally without the desktop application.
//! Library access stays with the Spotify provider; this module owns only audio and its setup.
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use async_trait::async_trait;
use futures::{SinkExt as _, StreamExt as _};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::{PlaybackConfig, PlaybackEvent, PlaybackEvents, PlaybackFactory, Player, credentials};

const START_TIMEOUT: Duration = Duration::from_secs(20);
/// Avoid competing pairing/playback processes sharing one Soloist session in this app.
static PROCESS: std::sync::OnceLock<std::sync::Arc<tokio::sync::Mutex<()>>> =
    std::sync::OnceLock::new();

fn process_lock() -> Result<tokio::sync::OwnedMutexGuard<()>> {
    PROCESS
        .get_or_init(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
        .clone()
        .try_lock_owned()
        .context("Soloist is busy; stop playback or cancel pairing first")
}

const POLL: Duration = Duration::from_millis(100);
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Private Soloist settings. The API key is never written to settings.json or debug logs.
#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub enabled: bool,
    pub executable: PathBuf,
    pub api_key: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            executable: PathBuf::from("soloist"),
            api_key: String::new(),
        }
    }
}

impl Config {
    /// Reads the private configuration, falling back to the fork's Soloist default.
    pub fn load() -> Self {
        std::fs::read(credentials::dir("soloist").join("config.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default()
    }

    /// Atomically saves settings with owner-only access using the existing credential store.
    pub fn save(&self) -> Result<()> {
        credentials::write(
            &credentials::dir("soloist").join("config.json"),
            &serde_json::to_vec(self)?,
        )
    }

    /// Starts Spotify Connect pairing and exits after Spotify has stored the session.
    /// Dropping this future terminates the pairing child.
    pub async fn pair(&self) -> Result<()> {
        let _lock = process_lock()?;
        let mut child = self
            .command()?
            .arg("--pair")
            .spawn()
            .context("cannot start Soloist pairing")?;
        let status = child.wait().await?;
        if !status.success() {
            bail!(
                "Soloist pairing failed (exit {:?}); check your key and build expiry",
                status.code()
            );
        }
        Ok(())
    }

    fn data_dir(&self) -> PathBuf {
        credentials::dir("soloist").join("session")
    }

    fn command(&self) -> Result<Command> {
        if self.api_key.trim().is_empty() {
            bail!("configure your Soloist API key in Settings → Playback first");
        }
        if self.executable.as_os_str().is_empty() {
            bail!("configure the Soloist executable first");
        }
        // Prepare the private parent using the same secure store as provider credentials.
        credentials::write(&credentials::dir("soloist").join(".private"), b"")?;
        let mut command = Command::new(&self.executable);
        command
            .args(["--device-name", "Sonora Soloist", "--api-key"])
            .arg(self.api_key.trim())
            .arg("--data-dir")
            .arg(self.data_dir())
            .arg("--cache-dir")
            .arg(credentials::root().join("soloist"))
            .args(["--cache-size", "100"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        Ok(command)
    }
}

/// Pairing task whose cancellation terminates its child instead of detaching it.
pub struct Pairing(tokio::task::JoinHandle<Result<()>>);

impl Pairing {
    pub fn start(config: Config, runtime: &tokio::runtime::Handle) -> Self {
        Self(runtime.spawn(async move { config.pair().await }))
    }

    pub async fn wait(&mut self) -> Result<()> {
        (&mut self.0)
            .await
            .context("Soloist pairing task stopped")?
    }
}

impl Drop for Pairing {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Builds the audio bridge on Sonora's existing I/O runtime.
pub struct Factory {
    config: Config,
    runtime: tokio::runtime::Handle,
}

impl Factory {
    /// Capture the app's I/O handle while the provider session is built on tokio. `start`
    /// itself runs on GPUI's main thread, which has no current tokio reactor.
    pub fn new(config: Config, runtime: tokio::runtime::Handle) -> Self {
        Self { config, runtime }
    }
}

impl PlaybackFactory for Factory {
    fn start(&self, config: PlaybackConfig) -> (Box<dyn Player>, Box<dyn PlaybackEvents>) {
        let (commands, receiver) = unbounded_channel();
        let (events, output) = unbounded_channel();
        let setup = self.config.clone();
        let task = self.runtime.spawn(async move {
            let mut worker = Worker {
                setup,
                gain: config.gain,
                current: None,
                events,
                interval: config.position_interval.max(Duration::from_millis(100)),
            };
            if let Err(error) = worker.run(receiver).await {
                log::error!("soloist: {error:#}");
                worker.emit(PlaybackEvent::Unavailable { id: worker.id() });
            }
        });
        (
            Box::new(Bridge { commands, task }),
            Box::new(Events(output)),
        )
    }
}

enum Control {
    Load(String, Duration, bool),
    Play,
    Pause,
    Seek(Duration),
    Gain(f32),
}

struct Bridge {
    commands: UnboundedSender<Control>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Bridge {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Player for Bridge {
    fn load(&self, id: &str, at: Duration, _seamless: bool) -> Result<()> {
        self.commands
            .send(Control::Load(id.to_owned(), at, false))
            .context("Soloist worker stopped")
    }
    fn load_paused_at(&self, id: &str, at: Duration) -> Result<()> {
        self.commands
            .send(Control::Load(id.to_owned(), at, true))
            .context("Soloist worker stopped")
    }
    fn preload(&self, _id: &str, _segue: bool) -> Result<()> {
        Ok(())
    }
    fn play(&self) {
        let _ = self.commands.send(Control::Play);
    }
    fn pause(&self) {
        let _ = self.commands.send(Control::Pause);
    }
    fn seek(&self, at: Duration) {
        let _ = self.commands.send(Control::Seek(at));
    }
    fn set_gain(&self, gain: f32) {
        let _ = self.commands.send(Control::Gain(gain));
    }
}

struct Events(UnboundedReceiver<PlaybackEvent>);
#[async_trait]
impl PlaybackEvents for Events {
    async fn next(&mut self) -> Option<PlaybackEvent> {
        self.0.recv().await
    }
}

/// One single-track Soloist process. Process exit is the end-of-track signal, so autoplay,
/// repeated songs and queue transitions cannot silently diverge from Sonora's own queue.
struct Running {
    child: Child,
    _lock: tokio::sync::OwnedMutexGuard<()>,
    socket: Socket,
    id: String,
    at: Duration,
    anchor: Instant,
    deadline: Instant,
    playing: bool,
    ready: bool,
    prepared: bool,
    start_at: Duration,
    paused: bool,
    seek_pending: bool,
}

struct Worker {
    setup: Config,
    gain: f32,
    current: Option<Running>,
    events: UnboundedSender<PlaybackEvent>,
    interval: Duration,
}

impl Worker {
    fn id(&self) -> Option<String> {
        self.current.as_ref().map(|track| track.id.clone())
    }
    fn emit(&self, event: PlaybackEvent) {
        let _ = self.events.send(event);
    }

    async fn run(&mut self, mut commands: UnboundedReceiver<Control>) -> Result<()> {
        let mut clock = tokio::time::interval(self.interval);
        clock.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if self.current.is_none() {
                let Some(command) = commands.recv().await else {
                    return Ok(());
                };
                self.control(command).await?;
                continue;
            }
            let current = self.current.as_mut().expect("checked above");
            tokio::select! {
                command = commands.recv() => {
                    let Some(command) = command else { return Ok(()) };
                    self.control(command).await?;
                }
                status = current.child.wait() => {
                    let status = status?;
                    let id = self.id();
                    self.current = None;
                    if status.success() {
                        self.emit(PlaybackEvent::Ended { id });
                    } else {
                        log::error!("soloist: playback exited with {:?} (10 means the build expired)", status.code());
                        self.emit(PlaybackEvent::Unavailable { id });
                    }
                }
                message = current.socket.next() => {
                    match message {
                        Some(Ok(Message::Text(text))) => {
                            let value: Value = serde_json::from_str(&text).context("cannot read Soloist event")?;
                            self.update(value).await?;
                        }
                        Some(Ok(Message::Ping(bytes))) => current.socket.send(Message::Pong(bytes)).await?,
                        Some(Err(_)) | None | Some(Ok(Message::Close(_))) => {
                            // The WebSocket closes at the end of single-track playback. Wait for
                            // the process to report its exit, rather than guessing from position.
                            let status = tokio::time::timeout(Duration::from_secs(3), current.child.wait()).await;
                            let id = self.id();
                            self.current = None;
                            match status {
                                Ok(Ok(status)) if status.success() => self.emit(PlaybackEvent::Ended { id }),
                                _ => self.emit(PlaybackEvent::Unavailable { id }),
                            }
                        }
                        _ => {}
                    }
                }
                _ = clock.tick() => {
                    if !current.ready && Instant::now() > current.deadline {
                        let id = Some(current.id.clone());
                        log::error!("soloist: track did not become ready within 20 seconds");
                        self.current = None;
                        self.emit(PlaybackEvent::Unavailable { id });
                        continue;
                    }
                    if current.ready && current.playing {
                        let _ = self.events.send(PlaybackEvent::Position { id: Some(current.id.clone()), at: current.at + current.anchor.elapsed() });
                    }
                }
            }
        }
    }

    async fn control(&mut self, command: Control) -> Result<()> {
        match command {
            Control::Load(id, at, paused) => {
                if let Some(mut old) = self.current.take() {
                    old.child.kill().await.ok();
                    old.child.wait().await.ok();
                }
                self.emit(PlaybackEvent::Loading {
                    id: Some(id.clone()),
                    at,
                });
                match self.start(id.clone(), at, paused).await {
                    Ok(track) => self.current = Some(track),
                    Err(error) => {
                        log::error!("soloist: {error:#}");
                        self.emit(PlaybackEvent::Unavailable { id: Some(id) });
                    }
                }
            }
            Control::Play | Control::Pause => {
                let paused = matches!(command, Control::Pause);
                if let Some(track) = self.current.as_mut() {
                    track.paused = paused;
                }
                self.send(json!({"command":if paused { "pause" } else { "play" }}))
                    .await?;
            }
            Control::Seek(at) => {
                if let Some(current) = self.current.as_mut() {
                    current.seek_pending = true;
                }
                self.send(json!({"command":"seek", "position_ms":at.as_millis() as u64}))
                    .await?;
            }
            Control::Gain(gain) => {
                self.gain = gain;
                if self.current.as_ref().is_some_and(|track| track.ready) {
                    self.send(json!({"command":"set_volume", "volume":volume(gain)}))
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn start(&self, id: String, at: Duration, paused: bool) -> Result<Running> {
        let lock = process_lock()?;
        let dir = self.setup.data_dir();
        // A previous process can leave discovery files on abnormal termination.
        let _ = tokio::fs::remove_file(dir.join("ws.port")).await;
        let mut child = self
            .setup
            .command()?
            .arg("--single-track")
            .arg(uri(&id))
            .args(["--initial-volume", "0", "--ws", "127.0.0.1:0"])
            .spawn()
            .context("cannot start Soloist; install the official executable and pair first")?;
        let deadline = Instant::now() + START_TIMEOUT;
        loop {
            if let Some(status) = child.try_wait()? {
                bail!(
                    "Soloist stopped before playback (exit {:?}); pair it first, check the key and build expiry",
                    status.code()
                );
            }
            if let Ok(port) = tokio::fs::read_to_string(dir.join("ws.port")).await
                && let Ok(port) = port.trim().parse::<u16>()
                && port != 0
                && let Ok((socket, _)) = connect_async(format!("ws://127.0.0.1:{port}")).await
            {
                return Ok(Running {
                    child,
                    _lock: lock,
                    socket,
                    id,
                    at,
                    anchor: Instant::now(),
                    deadline: Instant::now() + START_TIMEOUT,
                    playing: false,
                    ready: false,
                    prepared: false,
                    start_at: at,
                    paused,
                    seek_pending: false,
                });
            }
            if Instant::now() >= deadline {
                bail!("Soloist did not open its local control socket within 20 seconds");
            }
            tokio::time::sleep(POLL).await;
        }
    }

    async fn send(&mut self, mut message: Value) -> Result<()> {
        if let Some(current) = self.current.as_mut() {
            message["type"] = json!("command");
            current
                .socket
                .send(Message::Text(message.to_string().into()))
                .await
                .context("cannot send Soloist command")?;
        }
        Ok(())
    }

    async fn update(&mut self, event: Value) -> Result<()> {
        let kind = event["type"].as_str().unwrap_or("");
        if kind == "error" {
            bail!("Soloist rejected a playback command; check pairing and supported actions");
        }
        if kind == "auth_state" {
            if event["logged_in"] == true {
                self.send(json!({"command":"get_state"})).await?;
            }
            return Ok(());
        }
        if kind == "playback_changed" || kind == "track_changed" {
            self.send(json!({"command":"get_state"})).await?;
            return Ok(());
        }
        if !matches!(kind, "playback_state" | "position_sync") {
            return Ok(());
        }
        let Some(current) = self.current.as_mut() else {
            return Ok(());
        };
        if let Some(ms) = event["position"]["position_ms"].as_u64() {
            current.at = Duration::from_millis(ms);
            current.anchor = Instant::now();
        }
        let id = Some(current.id.clone());
        if kind == "position_sync" {
            if let Some(speed) = event["position"]["speed"].as_f64() {
                current.playing = speed > 0.;
            }
            if current.seek_pending {
                current.seek_pending = false;
                let at = current.at;
                self.emit(PlaybackEvent::Seeked { id, at });
            }
            return Ok(());
        }
        if event["status"] == "buffering" {
            return Ok(());
        }
        let initial = !current.ready;
        if initial {
            let seek = !current.prepared && !current.start_at.is_zero();
            let pause = current.paused && event["status"] != "paused";
            current.prepared = true;
            let start_at = current.start_at;
            let positioned =
                start_at.is_zero() || current.at.abs_diff(start_at) <= Duration::from_millis(500);
            if seek {
                self.send(json!({"command":"seek", "position_ms":start_at.as_millis() as u64}))
                    .await?;
            }
            if pause {
                self.send(json!({"command":"pause"})).await?;
            }
            if seek
                || pause
                || !positioned
                || (!current_paused(self) && event["status"] != "playing")
            {
                self.send(json!({"command":"get_state"})).await?;
                return Ok(());
            }
            self.current.as_mut().expect("current track").ready = true;
            self.send(json!({"command":"set_volume", "volume":volume(self.gain)}))
                .await?;
        }
        let current = self
            .current
            .as_mut()
            .expect("current is retained across commands");
        let was_playing = current.playing;
        current.playing = event["status"] == "playing";
        let at = current.at;
        let duration = event["item"]["decorations"]["playback"]["duration_ms"].as_u64();
        if let Some(ms) = duration {
            self.emit(PlaybackEvent::Length {
                id: id.clone(),
                duration: Duration::from_millis(ms),
            });
        }
        match event["status"].as_str() {
            Some("playing") if initial || !was_playing => {
                self.emit(PlaybackEvent::Playing { id, at })
            }
            Some("paused") => self.emit(PlaybackEvent::Paused { id, at }),
            _ => {}
        }
        Ok(())
    }
}

fn current_paused(worker: &Worker) -> bool {
    worker.current.as_ref().is_some_and(|track| track.paused)
}

fn volume(gain: f32) -> u8 {
    // Player receives a linear gain from state::playback's 50 dB taper. Soloist expects
    // the user's volume percentage and applies its own taper, so undo Sonora's first.
    if !gain.is_finite() || gain <= 0. {
        return 0;
    }
    ((1. + 20. * gain.log10() / 50.).clamp(0., 1.) * 100.).round() as u8
}
fn uri(id: &str) -> String {
    match id.starts_with("spotify:") {
        true => id.to_owned(),
        false => format!("spotify:track:{id}"),
    }
}
