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
pub struct Factory(pub Config);

impl PlaybackFactory for Factory {
    fn start(&self, config: PlaybackConfig) -> (Box<dyn Player>, Box<dyn PlaybackEvents>) {
        let (commands, receiver) = unbounded_channel();
        let (events, output) = unbounded_channel();
        let setup = self.0.clone();
        let task = tokio::spawn(async move {
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
    playing: bool,
    ready: bool,
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
            Control::Play => self.send(json!({"command":"play"})).await?,
            Control::Pause => self.send(json!({"command":"pause"})).await?,
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
                    playing: false,
                    ready: false,
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
        if !current.ready && matches!(event["status"].as_str(), Some("playing" | "paused")) {
            current.ready = true;
            let start_at = current.start_at;
            let paused = current.paused;
            if !start_at.is_zero() {
                self.send(json!({"command":"seek", "position_ms":start_at.as_millis() as u64}))
                    .await?;
            }
            if paused {
                self.send(json!({"command":"pause"})).await?;
            }
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
            Some("playing") if !was_playing => self.emit(PlaybackEvent::Playing { id, at }),
            Some("paused") => self.emit(PlaybackEvent::Paused { id, at }),
            _ => {}
        }
        Ok(())
    }
}

fn volume(gain: f32) -> u8 {
    (gain.clamp(0., 1.) * 100.).round() as u8
}
fn uri(id: &str) -> String {
    match id.starts_with("spotify:") {
        true => id.to_owned(),
        false => format!("spotify:track:{id}"),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    /// Exercises the real WebSocket wire format without a Spotify account or API key.
    async fn harness() -> (
        Worker,
        WebSocketStream<TcpStream>,
        UnboundedReceiver<PlaybackEvent>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap()
        });
        let (socket, _) = connect_async(format!("ws://{address}")).await.unwrap();
        let socket_server = server.await.unwrap();
        let (events, output) = unbounded_channel();
        let lock = std::sync::Arc::new(tokio::sync::Mutex::new(()))
            .lock_owned()
            .await;
        let child = Command::new("sleep")
            .arg("30")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let worker = Worker {
            setup: Config::default(),
            gain: 0.42,
            events,
            interval: POLL,
            current: Some(Running {
                child,
                _lock: lock,
                socket,
                id: "track-id".to_owned(),
                at: Duration::ZERO,
                anchor: Instant::now(),
                playing: false,
                ready: false,
                start_at: Duration::ZERO,
                paused: false,
                seek_pending: false,
            }),
        };
        (worker, socket_server, output)
    }

    async fn read_command(socket: &mut WebSocketStream<TcpStream>) -> Value {
        let message = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        serde_json::from_str(message.to_text().unwrap()).unwrap()
    }

    #[tokio::test]
    async fn ready_snapshot_unmutes_and_announces_real_playback() {
        let (mut worker, mut socket, mut events) = harness().await;
        worker
            .update(json!({"type":"playback_state", "status":"buffering"}))
            .await
            .unwrap();
        assert!(events.try_recv().is_err());
        worker.update(json!({"type":"playback_state", "status":"playing", "position":{"position_ms":1200}, "item":{"decorations":{"playback":{"duration_ms":5000}}}})).await.unwrap();
        assert_eq!(
            read_command(&mut socket).await,
            json!({"type":"command", "command":"set_volume", "volume":42})
        );
        assert!(
            matches!(events.recv().await, Some(PlaybackEvent::Length {duration, ..}) if duration == Duration::from_secs(5))
        );
        assert!(
            matches!(events.recv().await, Some(PlaybackEvent::Playing {id:Some(id), at}) if id == "track-id" && at == Duration::from_millis(1200))
        );
        assert!(events.try_recv().is_err());
    }

    #[tokio::test]
    async fn seek_waits_for_position_confirmation() {
        let (mut worker, mut socket, mut events) = harness().await;
        worker
            .control(Control::Seek(Duration::from_secs(12)))
            .await
            .unwrap();
        assert_eq!(
            read_command(&mut socket).await,
            json!({"type":"command", "command":"seek", "position_ms":12000})
        );
        assert!(events.try_recv().is_err());
        worker
            .update(json!({"type":"position_sync", "position":{"position_ms":12000, "speed":1.0}}))
            .await
            .unwrap();
        assert!(
            matches!(events.recv().await, Some(PlaybackEvent::Seeked {at, ..}) if at == Duration::from_secs(12))
        );
    }

    #[tokio::test]
    async fn authentication_requests_state_without_sending_the_api_key() {
        let (mut worker, mut socket, _) = harness().await;
        worker
            .update(json!({"type":"auth_state", "logged_in":true}))
            .await
            .unwrap();
        assert_eq!(
            read_command(&mut socket).await,
            json!({"type":"command", "command":"get_state"})
        );
        worker
            .update(json!({"type":"error", "message":"command requires authentication"}))
            .await
            .unwrap_err();
    }
}
