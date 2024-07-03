use std::process::Command;

use super::{TranscodeSettings, Transcoder};

pub struct LibX264;

impl Transcoder for LibX264 {
    fn transcode(&self, settings: &TranscodeSettings, ff: &mut Command) -> anyhow::Result<()> {
        ff
        // convert to h264
        .arg("-c:v")
        .arg("libx264")
        // set CRF
        .arg("-crf")
        .arg(settings.crf.to_string())
        // set preset
        .arg("-preset")
        .arg(self.get_preset(settings.preset))
        // tune (TODO: make this settable)
        .arg("-tune")
        .arg("animation")
        // disable audio
        .arg("-an")
        // force consistent GOPs and I-frames
        .arg("-g")
        .arg(settings.gop_size_frames.to_string())
        .arg("-keyint_min")
        .arg(settings.gop_size_frames.to_string())
        // disable scene change detection
        .arg("-sc_threshold")
        .arg("0");

        if let Some(avc_params) = settings.encoder_params.as_ref() {
            ff.arg("-x264-params").arg(avc_params);
        }
        Ok(())
    }
    fn get_preset(&self, preset: u8) -> String {
        match preset { 
            0..=1 => "ultrafast",
            2..=3 => "superfast",
            4..=5 => "veryfast",
            6 => "faster",
            7 => "fast",
            8 => "medium",
            9..=11 => "slow",
            12..=13 => "slower",
            14..=16 => "veryslow",
            _ => "ultrafast",
        }.to_string()
    }
}