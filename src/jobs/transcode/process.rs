use std::{
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{anyhow, Context, Error};
use axum::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{debug, info, warn};

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
        // work fodler is "work/<job_id>"
        let workdir = PathBuf::from(format!("temp/{}", jctx.id));
        let _ = tokio::fs::create_dir_all(workdir.clone()).await;
        // extract subtitles
        let subs = extract_ass_from_mkv(self.path.clone(), workdir.join("sub"), &streams).await?;
        //let mut av = extract_av_from_mkv(self.path.clone(), workdir.clone(), &streams).await?;
        let res = transcode_multiple_res(&self.path, &workdir, &streams, &jctx, state)?;
        // does res match the one that we literally just generated?
        debug!("res!!!: {:?}", res);

        // check that out dir _doesn't_ exist
        let out_dir = PathBuf::from(format!("out/{}", jctx.id));
        if tokio::fs::metadata(out_dir.clone()).await.is_ok() {
            warn!("out dir already exists, deleting...");
            tokio::fs::remove_dir_all(out_dir.clone()).await?;
        }

        let out_dir = PathBuf::from(format!("out/{}", jctx.id));

        let _ = tokio::fs::create_dir_all(out_dir.clone()).await;
        debug!("Segmenting {}", workdir.to_str().unwrap());
        dbg!(&res);
        let _ = fragment_and_segment(workdir, out_dir, &res)?;

        Ok(json!({"subtitles": subs, "renditions": res}))
    }
}

fn fragment_and_segment(
    workdir: PathBuf,
    out_dir: PathBuf,
    renditions: &[TranscodeEntry],
) -> Result<Vec<TranscodeEntry>, Error> {
    let mut ret_renditions: Vec<TranscodeEntry> = Vec::new();
    // Fragment all renditions
    for entry in renditions {
        // mp4fragment "$file" "f_$file"
        debug!(
            "fragmenting {}/{}",
            workdir
                .to_str()
                .context("could not convert path to string?")?,
            entry.filename
        );
        Command::new("mp4fragment")
            .arg(format!(
                "{}/{}",
                workdir
                    .to_str()
                    .context("could not convert path to string?")?,
                entry.filename
            ))
            .arg(format!(
                "{}/f_{}",
                workdir.to_str().unwrap(),
                entry.filename
            ))
            .output()?;
        ret_renditions.push(TranscodeEntry {
            filename: format!("f_{}", entry.filename),
            resolution: entry.resolution,
            codec_type: entry.codec_type,
        })
    }
    // get work directory string
    let workdir_str = workdir
        .to_str()
        .context("could not convert path to string?")?;
    // calculate file list based off of work directory and renditions
    let file_list: String = ret_renditions
        .iter()
        .map(|e| format!("{}/{}", workdir_str, e.filename))
        .collect::<Vec<String>>()
        .join(" ");
    debug!("file list: {}", file_list);
    let out_dir_str = out_dir
        .to_str()
        .context("could not convert path to string?")?;
    // mp4dash --use-segment-timeline --no-split f_*.m* -o export
    debug!(
        "mp4dash command: mp4dash --use-segment-timeline --no-split {} -o {}",
        file_list, out_dir_str
    );
    Command::new("mp4dash")
        .args([
            "--hls",
            "--force",
            "--use-segment-timeline",
            "--no-split",
            "--rename-media",
            "--hls-master-playlist-name=main.m3u8",
            &file_list,
            "-o",
            out_dir_str,
        ])
        .output()?;
    Ok(ret_renditions)
}

// not using for now b/c GOP issues
#[allow(dead_code)]
async fn extract_av_from_mkv(
    path: PathBuf,
    workdir: PathBuf,
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
                workdir.to_str().context("could not get output folder")?,
                resolution
            ),
            "-c",
            "copy",
            "-map",
            "0:a:0",
            &format!(
                "{}/{}.m4a",
                workdir.to_str().context("could not get output folder")?,
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

#[derive(Default, Serialize, Deserialize)]
struct MultiResProgress {
    fps: f64,
    bitrate: String,
    total_size_b: u64,
    out_time_ms: u64,
    speed: String,
    progress: f64,
}
fn transcode_multiple_res(
    path: &PathBuf,
    workdir: &Path,
    streams: &[Stream],
    jctx: &JobCtx,
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
    // avc has difficulties with web streaming right now
    let config_resolutions_avc_height = []; //720, 360];
    let config_resolutions_av1_height = [2160, 1440, 1080, 720, 480];
    let config_audio_bitrates = ["128k"];
    let mut resolutions: Vec<Resolution> = Vec::new();
    let mut bitrates: Vec<String> = Vec::new();

    // combine resolutions
    let mut config_resolutions_height = config_resolutions_avc_height.to_vec();
    config_resolutions_height.extend_from_slice(&config_resolutions_av1_height);

    // prob a better way to do this
    for res in config_resolutions_avc_height {
        // if resolution is less than video resolution
        if Some(res) <= stream.height {
            resolutions.push(Resolution {
                height: res,
                codec: "avc".to_owned(),
            });
        }
    }

    for res in config_resolutions_av1_height {
        // if resolution is less than video resolution
        if Some(res) <= stream.height {
            resolutions.push(Resolution {
                height: res,
                codec: "av1".to_owned(),
            });
        }
    }

    for res in config_audio_bitrates {
        // if resolution is less than video resolution
        if *res == *bitrate {
            bitrates.push(res.to_string());
        }
    }

    // do all the resolutions already exist? if so, return the entries
    let mut tc = Vec::new();
    for res in &resolutions {
        if workdir
            .join(format!("{}p-{}.mp4", res.height, res.codec))
            .exists()
        {
            // build a tc entry
            tc.push(TranscodeEntry {
                filename: format!("{}p-{}.mp4", res.height, res.codec),
                resolution: res.height,
                codec_type: CodecType::Video,
            });
        }
    }

    for b in &bitrates {
        if workdir.join(format!("{}.m4a", b)).exists() {
            // build a tc entry
            tc.push(TranscodeEntry {
                filename: format!("{}.m4a", b),
                resolution: 0,
                codec_type: CodecType::Audio,
            });
        }
    }

    // we transcode all at once, so doing this should be okay
    if !tc.is_empty() {
        return Ok(tc);
    }

    if resolutions.is_empty() {
        return Err(anyhow!("Video smaller than lowest transcoded resolution"));
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
        filter_complex.push_str(&format!(
            "[v{}]scale=-2:{}[v{}out]",
            i + 1,
            res.height,
            i + 1
        ));
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
    // calculate GOP
    let gop_size = (fps * gop_duration_seconds).round() as i32;

    // transcode all resolutions
    for (i, res) in resolutions.iter().enumerate() {
        debug!("Building res {} ({})", res.height, res.codec);
        ff.arg("-map").arg(format!("[v{}out]", i + 1));
        let settings = TranscodeSettings {
            crf: match res.codec.as_str() {
                "av1" => 32,
                "avc" => 24,
                _ => return Err(anyhow!("unsupported codec")),
            },
            preset: match res.codec.as_str() {
                "av1" => 6,
                "avc" => 5,
                _ => return Err(anyhow!("unsupported codec")),
            },
            encoder_params: match res.codec.as_str() {
                "av1" => Some(
                    "tune=2:enable-overlays=1:film-grain=8:adaptive-film-grain=1:lp=2".to_owned(),
                ),
                "avc" => None,
                _ => return Err(anyhow!("unsupported codec")),
            },
            ..Default::default()
        };
        let transcoder: Box<dyn Transcoder> = match res.codec.as_str() {
            "av1" => Box::new(SvtAv1),
            "avc" => Box::new(LibX264),
            _ => return Err(anyhow!("unsupported codec")),
        };
        transcoder.transcode(&settings, &mut ff)?;

        ff.arg("-an")
            // force consistent GOPs and I-frames
            .arg("-g")
            .arg(gop_size.to_string())
            .arg("-keyint_min")
            .arg(gop_size.to_string())
            .arg("-pix_fmt")
            .arg("yuv420p10le")
            // disable scene change detection
            .arg("-sc_threshold")
            .arg("0")
            .arg(format!(
                "{}/{}p-{}.mp4",
                workdir.to_string_lossy(),
                res.height,
                res.codec
            ));

        transc_entries.push(TranscodeEntry {
            filename: format!("{}p-{}.mp4", res.height, res.codec),
            resolution: res.height,
            codec_type: CodecType::Video,
        });
    }

    dbg!(ff.get_args().collect::<Vec<_>>());

    // add audio
    for &br in config_audio_bitrates.iter() {
        ff.arg("-vn")
            .arg("-c:a")
            .arg("aac")
            .arg("-b:a")
            .arg(br)
            .arg(format!("{}/{}.m4a", workdir.to_string_lossy(), br));

        transc_entries.push(TranscodeEntry {
            filename: format!("{}.m4a", br),
            resolution: 0,
            codec_type: CodecType::Audio,
        });
    }

    dbg!(&stream.tags);

    dbg!(&transc_entries);
    // get number of frames
    let frames = match stream.tags {
        Some(ref tags) => {
            if let Some(frame) = &tags.number_of_frames_eng {
                frame.parse::<i64>().unwrap_or_default()
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

    let mut progress = MultiResProgress {
        fps: 0.0,
        bitrate: "".to_string(),
        total_size_b: 0,
        out_time_ms: 0,
        speed: 0.to_string(),
        progress: 0.0,
    };

    for line in reader.lines() {
        if let Some((key, value)) = parse_line(&line?) {
            match (key, value) {
                ("frame", v) => {
                    progress.progress = v.parse::<i64>().unwrap_or(0) as f64 / frames as f64
                }
                ("fps", v) => progress.fps = v.parse().unwrap_or(0.0),
                ("bitrate", v) => progress.bitrate = v.to_string(),
                ("out_time_us", v) => progress.out_time_ms = v.parse().unwrap_or(0),
                ("speed", v) => progress.speed = v.to_string(),
                ("total_size", v) => progress.total_size_b = v.parse().unwrap_or(0),
                ("progress", v) if v != "continue" => break,
                ("progress", _) => jctx.update(state, &json!(progress))?,
                _ => {}
            }
        }
    }

    let status = ffe.wait()?;

    info!("transcoding complete - status: {:?}", status.code());

    debug!("Returning entries: {:?}", transc_entries);

    Ok(transc_entries)
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
    workdir: PathBuf,
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
        let _ = tokio::fs::create_dir_all(workdir.clone()).await;
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
                                workdir.to_str().context("could not get output folder")?,
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
