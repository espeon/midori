use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub mod filesystem;
pub mod queue_torrent;
pub mod transcode;

/// Output when running ffprobe -v error -print_format json -show_streams <video>.mkv
#[derive(Serialize, Deserialize)]
pub struct FfProbeStreamsOutput {
    streams: Option<Vec<Stream>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Stream {
    index: Option<i64>,
    codec_name: Option<String>,
    codec_long_name: Option<String>,
    profile: Option<String>,
    codec_type: Option<CodecType>,
    codec_tag_string: Option<String>,
    codec_tag: Option<String>,
    width: Option<i64>,
    height: Option<i64>,
    coded_width: Option<i64>,
    coded_height: Option<i64>,
    closed_captions: Option<i64>,
    has_b_frames: Option<i64>,
    sample_aspect_ratio: Option<String>,
    display_aspect_ratio: Option<String>,
    pix_fmt: Option<String>,
    level: Option<i64>,
    color_range: Option<String>,
    color_space: Option<String>,
    color_transfer: Option<String>,
    color_primaries: Option<String>,
    chroma_location: Option<String>,
    field_order: Option<String>,
    refs: Option<i64>,
    is_avc: Option<String>,
    nal_length_size: Option<String>,
    r_frame_rate: Option<String>,
    avg_frame_rate: Option<String>,
    time_base: Option<String>,
    start_pts: Option<i64>,
    start_time: Option<String>,
    bits_per_raw_sample: Option<String>,
    disposition: Option<HashMap<String, i64>>,
    tags: Option<Tags>,
    sample_fmt: Option<String>,
    sample_rate: Option<String>,
    channels: Option<i64>,
    channel_layout: Option<String>,
    bits_per_sample: Option<i64>,
    duration_ts: Option<i64>,
    duration: Option<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum CodecType {
    Attachment,
    Audio,
    Subtitle,
    Video,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Tags {
    #[serde(rename = "BPS", alias = "BPS-eng")]
    bps_eng: Option<String>,
    #[serde(rename = "DURATION", alias = "DURATION-eng")]
    duration_eng: Option<String>,
    #[serde(rename = "NUMBER_OF_FRAMES", alias = "NUMBER_OF_FRAMES-eng")]
    number_of_frames_eng: Option<String>,
    // both NUMBER_OF_BYTES and NUMBER_OF_BYTES-ENG
    #[serde(rename = "NUMBER_OF_BYTES", alias = "NUMBER_OF_BYTES-eng")]
    number_of_bytes_eng: Option<String>,
    #[serde(
        rename = "_STATISTICS_WRITING_APP",
        alias = "_STATISTICS_WRITING_APP-eng"
    )]
    statistics_writing_app_eng: Option<String>,
    #[serde(
        rename = "_STATISTICS_WRITING_DATE_UTC",
        alias = "_STATISTICS_WRITING_DATE_UTC-eng"
    )]
    statistics_writing_date_utc_eng: Option<String>,
    #[serde(rename = "_STATISTICS_TAGS", alias = "_STATISTICS_TAGS-eng")]
    statistics_tags_eng: Option<String>,
    language: Option<String>,
    title: Option<String>,
    filename: Option<String>,
    mimetype: Option<String>,
}
