use std::{
    io::{BufRead, BufReader},
    path::PathBuf,
    process::{Command, Stdio},
};

use anyhow::{anyhow, Context, Error};
use axum::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{debug, error, info};

use crate::{
    jobs::{
        transcode::{av1::SvtAv1, avc::LibX264, TranscodeSettings, Transcoder},
        CodecType, FfProbeStreamsOutput, Stream,
    },
    services::job::{Job, JobCtx, State},
};

#[derive(Default, Serialize, Deserialize)]
pub struct TranscodeJob {
    path: PathBuf,
}

impl TranscodeJob {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

/// Three steps to transcode:
/// 1. Extract video/audio/subs from mkv container
/// 2. Split by keyframe
/// 3. Segment
#[async_trait]
#[typetag::serde]
impl Job for TranscodeJob {
    async fn run(&self, state: &State, jctx: JobCtx) -> Result<serde_json::Value, Error> {
        let probe = Command::new("ffprobe")
            .args([
                "-v",
                "error",
                "-print_format",
                "json",
                "-show_streams",
                self.path.to_str().unwrap(),
            ])
            .output()?;
        let output: FfProbeStreamsOutput = serde_json::from_slice(&probe.stdout)?;
        let streams = output.streams.context("could not find streams")?;
        // output fodler is "out/<job_id>"
        let output_folder = PathBuf::from(format!("out/{}", jctx.id));
        let _ = tokio::fs::create_dir_all(output_folder.clone()).await;
        let subs =
            extract_ass_from_mkv(self.path.clone(), output_folder.join("sub"), &streams).await?;
        //let mut av = extract_av_from_mkv(self.path.clone(), output_folder.clone(), &streams).await?;
        let res =
            transcode_multiple_res(self.path.clone(), output_folder, &streams, jctx, state).await?;
        let renditions = res; //av.append(&mut res);
        Ok(json!({"subtitles": subs, "renditions": renditions}))
    }
}

#[allow(dead_code)]
async fn extract_av_from_mkv(
    path: PathBuf,
    output_folder: PathBuf,
    streams: &[Stream],
) -> Result<Vec<TranscodeEntry>, Error> {
    let resolution = streams
        .iter()
        .find(|s| s.codec_type == Some(CodecType::Video))
        .context("could not find video stream")?
        .height
        .context("could not get height of video stream")?;

    let bitrate_str = streams
        .iter()
        .find(|s| s.codec_type == Some(CodecType::Audio))
        .context("could not find audio stream")?
        .tags
        .clone()
        .context("could not get tags from audio stream")?
        .bps_eng
        .context("could not get bitrate of audio stream")?;

    let bitrate_int: i64 = bitrate_str
        .clone()
        .parse()
        .context("could not parse audio bitrate")?;
    let bitrate = (bitrate_int / 1000).to_string() + "k";
    let _ = Command::new("ffmpeg")
        .args([
            "-i",
            path.to_str().context("could not convert path to string?")?,
            "-c",
            "copy",
            "-map",
            "0:v:0",
            &format!(
                "{}/{}p.mp4",
                output_folder
                    .to_str()
                    .context("could not get output folder")?,
                resolution
            ),
            "-c",
            "copy",
            "-map",
            "0:a:0",
            &format!(
                "{}/{}.m4a",
                output_folder
                    .to_str()
                    .context("could not get output folder")?,
                bitrate
            ),
        ])
        .output()
        .context("Failed to execute ffmpeg")?;

    Ok(vec![
        TranscodeEntry {
            filename: format!("{}p.mp4", resolution),
            resolution,
            codec_type: CodecType::Video,
        },
        TranscodeEntry {
            filename: format!("{}.m4a", bitrate),
            resolution,
            codec_type: CodecType::Audio,
        },
    ])
}

#[derive(Debug, Serialize, Deserialize)]
struct TranscodeEntry {
    filename: String,
    resolution: i64,
    codec_type: CodecType,
}

struct Resolution {
    height: i64,
    codec: String,
}

async fn transcode_multiple_res(
    path: PathBuf,
    output_folder: PathBuf,
    streams: &[Stream],
    jctx: JobCtx,
    state: &State,
) -> Result<Vec<TranscodeEntry>, Error> {
    // get first video track
    let stream = streams
        .iter()
        .find(|s| s.codec_type == Some(CodecType::Video))
        .context("could not find video stream")?;

    let bitrate_str = streams
        .iter()
        .find(|s| s.codec_type == Some(CodecType::Audio))
        .context("could not find audio stream")?
        .tags
        .clone()
        .context("could not get tags from audio stream")?
        .bps_eng
        .context("could not get bitrate of audio stream")?;

    let bitrate_int: i64 = bitrate_str
        .clone()
        .parse()
        .context("could not parse audio bitrate")?;
    let bitrate = (bitrate_int / 1000).to_string() + "k";
    // arr of res heights
    let config_resolutions_avc_height = [720, 360];
    let config_resolutions_av1_height = [2160, 1440, 1080, 720, 480];
    let config_audio_bitrates = ["128k", "64k"];
    let mut resolutions: Vec<Resolution> = Vec::new();
    let mut bitrates: Vec<String> = Vec::new();

    // combine resolutions
    let mut config_resolutions_height = config_resolutions_avc_height.to_vec();
    config_resolutions_height.extend_from_slice(&config_resolutions_av1_height);

    for res in config_resolutions_avc_height {
        // if resolution is less than video resolution
        if Some(res) <= stream.height {
            resolutions.push(Resolution { height: res, codec: "avc".to_owned() });
        }
    }

    for res in config_resolutions_av1_height {
        // if resolution is less than video resolution
        if Some(res) <= stream.height {
            resolutions.push(Resolution { height: res, codec: "av1".to_owned() });
        }
    }

    for res in config_audio_bitrates {
        // if resolution is less than video resolution
        if *res == *bitrate {
            bitrates.push(res.to_string());
        }
    }

    if resolutions.is_empty() {
        // not returning an error here in the case of the source video
        // actually being less than lowest transcoded resolution
        return Ok(vec![]);
    }
    // build ffmpeg command
    let mut ff = Command::new("ffmpeg");
    ff.arg("-i").arg(path);
    ff.arg("-progress").arg("pipe:1");
    let mut filter_complex = format!("[0:v]split={}", resolutions.len());
    for (i, _res) in resolutions.iter().enumerate() {
        filter_complex.push_str(&format!("[v{}]", i + 1));
    }
    filter_complex.push(';');
    for (i, res) in resolutions.iter().enumerate() {
        filter_complex.push_str(&format!("[v{}]scale=-2:{}[v{}out]", i + 1, res.height, i + 1));
        if i < resolutions.len() - 1 {
            filter_complex.push(';');
        }
    }
    ff.arg("-filter_complex").arg(filter_complex);

    let mut transc_entries: Vec<TranscodeEntry> = Vec::new();

    // get frame rate
    let frame_rate_fraction = stream
        .r_frame_rate
        .clone()
        .context("could not get frame rate")?;
    let fps = if frame_rate_fraction.contains('/') {
        let frf: Vec<&str> = frame_rate_fraction.split('/').collect();

        let top: f64 = frf[0].parse()?;
        let bottom: f64 = frf[1].parse()?;

        (top / bottom).round()
    } else {
        frame_rate_fraction.parse::<f64>()?.round()
    };
    let gop_duration_seconds: f64 = 3.0;
    let gop_size = (fps * gop_duration_seconds).round() as i32;

    for (i, res) in resolutions.iter().enumerate() {
        debug!("Building res {} ({})", res.height, res.codec);
        ff.arg("-map")
        .arg(format!("[v{}out]", i + 1));
        match res.codec.as_str() {
            "av1" => {
                let settings = TranscodeSettings {
                    crf: 30,
                    preset: 5,
                    ..Default::default()
                };
                SvtAv1.transcode(&settings, &mut ff, res.height, &output_folder)?;
            },
            "avc" => {
                let settings = TranscodeSettings {
                    crf: 16,
                    preset: 8,
                    ..Default::default()
                };
                LibX264.transcode(&settings, &mut ff, res.height, &output_folder)?;
            },
            _ => {
                anyhow::bail!("unsupported codec");
            }
        }

        ff.arg("-an")
        // force consistent GOPs and I-frames
        .arg("-g")
        .arg(gop_size.to_string())
        .arg("-keyint_min")
        .arg(gop_size.to_string())
        // disable scene change detection
        .arg("-sc_threshold")
        .arg("0")
        .arg(format!("{}/{}p-{}.mp4", output_folder.to_string_lossy(), res.height, res.codec));

        transc_entries.push(TranscodeEntry {
            filename: format!("{}p-{}.mp4", res.height, res.codec),
            resolution: res.height,
            codec_type: CodecType::Video,
        });
    }

    dbg!(ff.get_args().collect::<Vec<_>>());

    for &br in config_audio_bitrates.iter() {
        ff.arg("-vn")
            .arg("-c:a")
            .arg("aac")
            .arg("-b:a")
            .arg(br)
            .arg(format!("{}/{}.m4a", output_folder.to_string_lossy(), br));

        transc_entries.push(TranscodeEntry {
            filename: format!("{}.m4a", br),
            resolution: 0,
            codec_type: CodecType::Audio,
        });
    }

    dbg!(&stream.tags);

    // number of frames
    let frames = match stream.tags {
        Some(ref tags) => {
            if let Some(frame) = &tags.number_of_frames_eng {
                if let Ok(v) = frame.parse::<i64>() {
                    v
                } else {
                    0
                }
            } else {
                0
            }
        }
        None => 0,
    };

    if frames == 0 {
        debug!("Could not parse NUMBER_OF_FRAMES-eng tag");
    } else {
        debug!("Number of frames: {}", frames);
    }

    let mut ffe = ff.stdout(Stdio::piped()).spawn()?;

    let stdout = ffe
        .stdout
        .take()
        .context("Could not get stdout from child ffmpeg")?;
    let reader = BufReader::new(stdout);

    for lines in reader.lines() {
        if let Some((key, value)) = parse_line(&lines?) {
            debug!("key: {} - value: {}", key, value);
            if key == "frame" {
                let value = value.parse::<i64>().unwrap_or(0);

                debug!(
                    "Progress: {} - raw value: {}",
                    value as f64 / frames as f64,
                    value
                );
                jctx.update(
                    state,
                    &json!({
                        "progress": value as f64/frames as f64,
                        "frames": value,
                    }),
                )?;
            } else if key == "progress" && value != "continue" {
                break;
            }
        }
    }

    let status = ffe.wait()?;

    info!("transcoding complete - status: {:?}", status.code());

    Ok(vec![])
}

fn parse_line(line: &str) -> Option<(&str, &str)> {
    let trimmed = line.trim();
    let mut iter = trimmed.splitn(2, '=');

    let mut key = iter.next()?;
    key = key.trim_end();

    let mut value = iter.next()?;
    // Ffmpeg was putting in random spaces for some reason?
    value = value.trim_start();

    Some((key, value))
}

#[derive(Default, Serialize, Deserialize)]
struct SubtitleEntry {
    language: String,
    sub_type: String,
    filename: String,
}

async fn extract_ass_from_mkv(
    path: PathBuf,
    output_folder: PathBuf,
    streams: &[Stream],
) -> Result<Vec<SubtitleEntry>, Error> {
    // filter subs
    let subs = streams
        .iter()
        .filter(|s| s.codec_type == Some(CodecType::Subtitle))
        .collect::<Vec<_>>();

    let mut fin: Vec<SubtitleEntry> = Vec::new();
    dbg!(&subs);

    if !subs.is_empty() {
        let _ = tokio::fs::create_dir_all(output_folder.clone()).await;
    }

    for s in subs {
        // match by type - supported types: ass, srt, webvtt
        // wont support PGS subtitles as they can't easily be displayed without burning them in
        dbg!(&s.codec_name);
        dbg!(&s.tags);
        if let Some(sub_type) = &s.codec_name {
            // make sure we have tags
            if let Some(tags) = &s.tags {
                debug!(
                    "Processing index {}",
                    s.index.context("could not find index of stream")?
                );

                let language = match &tags.language {
                    Some(l) => l,
                    None => "unknown",
                };
                if sub_type == "ass"
                    || sub_type == "srt"
                    || sub_type == "webvtt"
                    || sub_type == "vtt"
                {
                    // extract language with ffmpeg
                    let ffmpeg_extract_output = Command::new("ffmpeg")
                        .args([
                            "-i",
                            path.to_str().context("could not convert path to string?")?,
                            "-map",
                            // the index of the stream
                            &format!("0:{}", s.index.context("could not find index of stream")?),
                            "-c:s",
                            sub_type,
                            format!(
                                "{}/{}.{}",
                                output_folder
                                    .to_str()
                                    .context("could not get output folder")?,
                                language,
                                sub_type
                            )
                            .as_str(),
                        ])
                        .output()
                        .context("Failed to execute ffmpeg for subtitle extraction")?;
                    if ffmpeg_extract_output.status.success() {
                        fin.push(SubtitleEntry {
                            language: language.to_string(),
                            sub_type: sub_type.to_string(),
                            filename: format!("{}.{}", language, sub_type),
                        })
                    } else {
                        debug!(
                            "ffmpeg output: {}",
                            String::from_utf8_lossy(&ffmpeg_extract_output.stdout)
                        );
                        return Err(anyhow!(
                            "ffmpeg error: {}",
                            String::from_utf8_lossy(&ffmpeg_extract_output.stderr)
                        ));
                    }
                }
            }
        }
    }

    Ok(fin)
}
