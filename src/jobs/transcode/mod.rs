mod process;
mod av1;
mod avc;

use std::process::Command;

// re-export from process
pub use process::*;


pub struct TranscodeSettings {
    /// Constant rate factor (0-63)
    crf: u16,
    /// Use hardware encoder if available
    //try_hardware_encoder: bool,
    /// 0-16, higher is faster but lower quality. Which preset it will use internally varies among encoders.
    preset: u8,
    // set before via filter piping
    //height: u16,
    gop_size_frames: u32,
    /// Specific encoder parameters - a colon separated list of key=value pairs
    /// e.g. for svt-av1: 'pin=4:tile-rows=2:tile-columns=2'
    encoder_params: Option<String>,
}

impl Default for TranscodeSettings {
    fn default() -> Self {
        Self {
            gop_size_frames: 120,
            crf: 23,
            //try_hardware_encoder: false,
            preset: 0,
            //height: 720,
            encoder_params: None
        }
    }
}

/// Generate transcode settings for ffmpeg
pub trait Transcoder {
    fn transcode(&self, settings: &TranscodeSettings, ff: &mut Command) -> anyhow::Result<()>;
    fn get_preset(&self, preset: u8) -> String;
}