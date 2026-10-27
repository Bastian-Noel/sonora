//! Native setup for the official Spotify playback process.
use gpui::prelude::*;
use gpui::{AnyElement, Context, div};
use i18n::t;
use music::soloist::{Config, Pairing};
use state::Io;
use ui::{ActiveTheme as _, Button, Switch, Text};

use super::SettingsView;

impl SettingsView {
    /// Saves setup before pairing or before the next application restart.
    fn save_soloist(&mut self, cx: &mut Context<Self>) -> Option<Config> {
        let mut config = Config::load();
        config.executable = self.soloist_path.read(cx).text().trim().into();
        config.api_key = self.soloist_key.read(cx).text().trim().to_owned();
        match config.save() {
            Ok(()) => {
                self.soloist_message = "settings-soloist-saved";
                cx.notify();
                Some(config)
            }
            Err(error) => {
                log::error!("soloist: cannot save configuration: {error:#}");
                self.soloist_message = "settings-soloist-save-error";
                cx.notify();
                None
            }
        }
    }

    pub(super) fn soloist_row(&self, cx: &mut Context<Self>) -> AnyElement {
        let theme = *cx.theme();
        let enabled = Config::load().enabled;
        div().flex().flex_col().gap_3()
            .child(self.row(t!("settings-soloist"),
                Switch::new("soloist-enabled", enabled)
                    .on_click(cx.listener(move |this, _, _, cx| {
                        if let Some(mut config) = this.save_soloist(cx) {
                            config.enabled = !enabled;
                            if config.save().is_err() { this.soloist_message = "settings-soloist-save-error"; }
                            cx.notify();
                        }
                    })).into_any_element()))
            .child(self.row(t!("settings-soloist-path"), self.soloist_path.clone().into_any_element()))
            .child(self.row(t!("settings-soloist-key"), self.soloist_key.clone().into_any_element()))
            .child(div().text_size(theme.text(Text::Small)).text_color(theme.muted_foreground)
                .child(i18n::lookup(self.soloist_message, None)))
            .child(div().flex().flex_wrap().gap_2()
                .child(Button::new("soloist-download").label(t!("settings-soloist-download")).outline()
                    .on_click(|_, _, cx| cx.open_url("https://developer.spotify.com/documentation/soloist/reference/downloads-and-updates")))
                .child(Button::new("soloist-dashboard").label(t!("settings-soloist-dashboard")).outline()
                    .on_click(|_, _, cx| cx.open_url("https://developer.spotify.com/dashboard")))
                .child(Button::new("soloist-save").label(t!("common-save")).outline()
                    .on_click(cx.listener(|this, _, _, cx| { this.save_soloist(cx); })))
                .child(Button::new("soloist-pair").label(t!("settings-soloist-pair")).primary()
                    .on_click(cx.listener(|this, _, _, cx| {
                        if this.soloist_pair.is_some() {
                            this.soloist_pair = None;
                            this.soloist_message = "settings-soloist-detail";
                            cx.notify();
                            return;
                        }
                        let Some(config) = this.save_soloist(cx) else { return };
                        let mut pair = Pairing::start(config, &Io::global(cx).handle());
                        this.soloist_message = "settings-soloist-pairing";
                        this.soloist_pair = Some(cx.spawn(async move |this, cx| {
                            let result = pair.wait().await;
                            if let Err(error) = &result { log::error!("soloist: pairing: {error:#}"); }
                            this.update(cx, |this, cx| {
                                this.soloist_message = if result.is_ok() { "settings-soloist-paired" } else { "settings-soloist-pair-error" };
                                this.soloist_pair = None;
                                cx.notify();
                            }).ok();
                        }));
                        cx.notify();
                    }))))
            .into_any_element()
    }
}
