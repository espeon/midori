use std::{path::Path, process::Command};

use super::{TranscodeSettings, Transcoder};

pub struct SvtAv1;

impl Transcoder for SvtAv1 {
    fn transcode(&self, settings: &TranscodeSettings, ff: &mut Command, res: i64, output_folder: &Path) -> anyhow::Result<()> {
        
        ff
          .arg("-c:v")
          .arg("libsvtav1")
          .arg("-crf")
          .arg(settings.crf.to_string())
          .arg("-preset")
          .arg(self.get_preset(settings.preset))
          .arg("-an")
          .arg(format!("{}/{}p.mp4", output_folder.to_string_lossy(), res));
        Ok(())
    }

    fn get_preset(&self, preset: u8) -> String {
        // convert preset directly to string 0-16
        preset.to_string()
    }
}

pub struct Av1Nvenc;

impl Transcoder for Av1Nvenc {
    fn transcode(&self, settings: &TranscodeSettings, ff: &mut Command, res: i64, output_folder: &Path) -> anyhow::Result<()> {
        ff
          .arg("-c:v")
          .arg("av1_nvenc")
          .arg("-preset")
          .arg(self.get_preset(settings.preset))
          .arg("-tune")
          .arg("hq")
          .arg("-an");
        Ok(())
    }

    fn get_preset(&self, preset: u8) -> String {
        match preset { 
            1..=2 => "p1",
            3..=5 => "p2",
            6..=7 => "p3",
            8 => "p4",
            9..=11 => "p5",
            12..=13 => "p6",
            14..=16 => "p7",
            _ => "default",
        }.to_string()
    }
}