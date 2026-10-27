# Sonora 0.44.2 with Spotify Soloist

Experimental Linux fork. Spotify library access stays in Sonora; Spotify audio is played by
Spotify's official, headless Soloist executable. The full Spotify desktop app is not needed.

## Setup

1. Download the Linux x86_64 build of Soloist from
   <https://developer.spotify.com/documentation/soloist/reference/downloads-and-updates>.
   Extract it and make the `soloist` executable executable. Soloist is proprietary and is not
   included in this fork's downloads.
2. Generate your personal Soloist API key in <https://developer.spotify.com/dashboard>.
3. Run this fork's `sonora` executable, and open **Settings → Playback → Spotify Soloist**.
4. Enter the absolute path to `soloist` and your personal API key. Save.
5. Stop any playing track, click **Pair**, and choose **Sonora Soloist** in Spotify's devices
   list on a phone on the same network. Pairing exits when the session is saved.
6. Restart Sonora and select a Spotify track. Sonora launches and controls Soloist automatically.

The switch defaults to Soloist in this fork. Turning it off and restarting returns to librespot
playback. Changes take effect after restarting. Each track uses Soloist's single-track mode;
Sonora owns the queue and starts the next process when the previous track exits. Pause, resume,
seek and volume go through a loopback WebSocket; playback progress drives Sonora's lyrics and
scrobbling. Stopping the engine or quitting Sonora terminates its child process.

The key is stored with owner-only permissions in
`$XDG_DATA_HOME/sonora/accounts/soloist/config.json`, alongside the private Soloist session.
It is not stored in this repository, a workflow, or `settings.json`. Soloist itself requires
passing the key on its command line; other processes running as your user can inspect that.

## Limits and validation

This is a new backend, not feature parity with librespot. Audio goes directly through the
system output selected by PipeWire/PulseAudio. Sonora's equalizer, spectrum, output picker,
stream quality controls, download cache and gapless transitions are not applied to Soloist.
Downloads and played-track caching are disabled for this backend. Sonora's separate Connect
receiver is disabled to avoid competing device state. Soloist single-track mode does not
accept playback transfers; it is controlled by Sonora's local socket instead.

Playback with a real account must be validated after generating a personal key and pairing.
A successful build alone does not establish that playback works on every Spotify account.

Soloist builds expire 90 days after their build date. Download a fresh executable from Spotify
before expiry. Sonora logs exit code 10 as an expired-build failure; no updater or binary
redistribution is included.

## Builds

The **Sonora Soloist Linux** workflow compiles and uploads an executable archive on pushes to
`soloist-playback`, pull requests targeting `main`, and manual runs. No Spotify credentials are
needed for compilation. Download its artifact from the Actions run. Unpack the archive and run
`./sonora-soloist/sonora`. Keep the regular Arch package installed for the system dependencies.
