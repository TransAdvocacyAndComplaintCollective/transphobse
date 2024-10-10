import os
import glob
import logging
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlparse
import xml.etree.ElementTree as ET
import yt_dlp
from pymediainfo import MediaInfo  # Used to check if MP4 contains embedded subtitles
import ffmpeg  # For using ffmpeg in Python
from utils.keywords import relative_keywords_score
import whisper
from utils.PluginBase import PluginBase

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BBCVideoOnDemand(PluginBase):
    def __init__(self, max_workers=None):
        self.media_dir = Path("./media")
        self.media_dir.mkdir(exist_ok=True)
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=max_workers or os.cpu_count())
        self.model = whisper.load_model("base")  # Use "base" or "small" for faster results, or "tiny" for even faster but less accurate

        # Load Vosk model once during initialization
        # self.model = self.load_vosk_model()


    def load_vosk_model(self):
        """Load the Vosk model once during initialization."""
        with self.lock:
            model_path = Path("./model/vosk-model-en-us-0.22").resolve()
            if not model_path.exists():
                raise FileNotFoundError(f"Model path {model_path} not found.")
            logger.info("Loading Vosk model...")
            return Model(str(model_path))  # Load the model once

    def on_start(self, crawler):
        """Called when the crawler starts."""
        logger.info("BBCVideoOnDemand plugin started")
        

    async def before_fetch(self, url, crawler):
        """Called before a URL is fetched. Checks if the URL is a BBC video or audio."""
        parsed_url = urlparse(url)
        
        # Ensure it's a BBC domain
        if "bbc.co.uk" in parsed_url.netloc:
            
            # Split the path to extract the episode or program ID and ensure it's a valid iPlayer or Sounds URL
            path_parts = parsed_url.path.strip("/").split("/")
            
            # Check for iPlayer episodes
            if len(path_parts) >= 3 and path_parts[0] == "iplayer" and path_parts[1] == "episode":
                episode_id = path_parts[2]  # Extract episode ID, e.g., p08yr4dk
                logger.info(f"BBC iPlayer episode detected: {episode_id} from URL: {url}")
                
                # Run process_video in a separate thread
                await self.run_in_thread(self.process_video, url)
                return False  # URL processed, don't continue crawling

            # Check for BBC Sounds play pages
            elif len(path_parts) >= 2 and path_parts[0] == "sounds" and path_parts[1] == "play":
                sound_id = path_parts[2] if len(path_parts) > 2 else None  # Extract sound ID if available
                logger.info(f"BBC Sounds play page detected: {sound_id} from URL: {url}")
                
                # Run process_audio in a separate thread
                await self.run_in_thread(self.process_audio, url)
                return False  # URL processed, don't continue crawling

        return True  # URL doesn't match the video/audio patterns, continue crawling

    def process_video(self, url):
        """Handles the full process of downloading, extracting, and generating subtitles for video."""
        video_filename = None
        try:
            # Step 1: Download the video using yt-dlp
            video_filename = self.download_video(url)
            if video_filename:
                # Step 2: Find the corresponding subtitle file (SRT, VTT, or TTML)
                subtitle_file = self.find_subtitle_file(video_filename)

                if subtitle_file:
                    logger.info(f"External subtitles found: {subtitle_file}")

                    # Step 3: Check if the subtitle is in TTML format and convert if necessary
                    if subtitle_file.endswith(".en.ttml"):
                        subtitle_file = self.process_ttml_subtitles(subtitle_file)

                    # Step 4: Check the subtitles for keywords
                    self.check_subtitle_keywords(subtitle_file)

                elif self.mp4_has_subtitles(video_filename):
                    # Step 5: If no external subtitles, check for embedded subtitles in MP4
                    logger.info(f"MP4 file contains embedded subtitles: {video_filename}")
                    self.check_embedded_subtitle_keywords(video_filename)

                else:
                    # Step 6: If no subtitles are found, extract audio and perform speech-to-text transcription
                    logger.warning("No subtitles found, extracting audio for transcription...")
                    audio_file = self.extract_audio_ffmpeg(video_filename)
                    if audio_file:
                        self.generate_subtitles(audio_file, video_filename)

        except Exception as e:
            # Log any errors that occur during processing
            logger.error(f"Error processing video {url}: {e}", exc_info=True)

        finally:
            # Ensure that any temporary files are cleaned up
            self.cleanup_files(video_filename)


    def process_audio(self, url):
        """Handles downloading and processing audio from BBC Sounds."""
        audio_filename = None
        try:
            audio_filename = self.download_audio(url)
            if audio_filename:
                # No subtitle files for audio, so proceed directly to transcription
                self.generate_subtitles(audio_filename, audio_filename)
        except Exception as e:
            logger.error(f"Error processing audio from {url}: {e}", exc_info=True)
        finally:
            # Ensure final cleanup
            self.cleanup_files(audio_filename)

    def download_video(self, url):
        """Handles downloading video and extracting subtitles."""
        logger.info(f"Downloading video from {url} using yt-dlp API...")
        video_filename = None
        try:
            ydl_opts = {
                'format': 'worst',  # Download the worst quality video
                'outtmpl': f'{self.media_dir}/%(title)s.%(ext)s',  # Save video to the media directory
                'writesubtitles': True,  # Try to download subtitles if available
                'subtitleslangs': ['en'],  # Download only English subtitles
                'quiet': True,
            }

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info_dict = ydl.extract_info(url, download=True)
                video_filename = ydl.prepare_filename(info_dict)

            return video_filename
        except Exception as e:
            logger.error(f"Error downloading video from {url}: {e}", exc_info=True)
            return None

    def download_audio(self, url):
        """Handles downloading audio from BBC Sounds."""
        logger.info(f"Downloading audio from {url} using yt-dlp API...")
        audio_filename = None
        try:
            ydl_opts = {
                'format': 'bestaudio/best',
                'outtmpl': f'{self.media_dir}/%(title)s.%(ext)s',  # Save audio to the media directory
                'quiet': True,
            }

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info_dict = ydl.extract_info(url, download=True)
                audio_filename = ydl.prepare_filename(info_dict)

            return audio_filename
        except Exception as e:
            logger.error(f"Error downloading audio from {url}: {e}", exc_info=True)
            return None

    def mp4_has_subtitles(self, video_filename):
        """Check if the MP4 file contains embedded subtitles using ffmpeg."""
        try:
            # Use ffmpeg's probe function to get information about the media file
            probe = ffmpeg.probe(video_filename)
            streams = probe.get('streams', [])
            for stream in streams:
                if stream.get('codec_type') == 'subtitle':
                    logger.info(f"Embedded subtitles found in MP4 file: {video_filename}")
                    return True
            logger.info(f"No embedded subtitles found in MP4 file: {video_filename}")
            return False
        except ffmpeg.Error as e:
            logger.error(f"Error checking for subtitles in {video_filename}: {e.stderr.decode()}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Unexpected error checking for subtitles in {video_filename}: {e}", exc_info=True)
            return False

    def process_ttml_subtitles(self, ttml_file):
        """Convert TTML to SRT for further processing."""
        output_srt = ttml_file.replace(".en.ttml", ".srt")
        try:
            logger.info(f"Converting TTML to SRT: {ttml_file}")

            tree = ET.parse(ttml_file)
            root = tree.getroot()

            # Namespace for TTML
            namespace = {'ttml': 'http://www.w3.org/ns/ttml'}

            with open(output_srt, 'w', encoding='utf-8') as srt_fp:
                count = 1
                for p in root.findall('.//ttml:p', namespace):
                    start_time = p.attrib.get('begin', '00:00:00.000')
                    end_time = p.attrib.get('end', '00:00:00.000')
                    text = ''.join(p.itertext()).strip()

                    # Write in SRT format
                    srt_fp.write(f"{count}\n")
                    srt_fp.write(f"{start_time} --> {end_time}\n")
                    srt_fp.write(f"{text}\n\n")
                    count += 1

            return output_srt
        except Exception as e:
            logger.error(f"Error converting TTML to SRT: {e}", exc_info=True)
            return None


    def find_subtitle_file(self, video_filename):
        """Finds the subtitle file based on the video filename."""
        base, _ = os.path.splitext(video_filename)
        for ext in ['.vtt', '.srt', '.ttml',".en.ttml"]:
            subtitle_file = f"{base}{ext}"
            if os.path.exists(subtitle_file):
                return subtitle_file
        return None

    def extract_audio_ffmpeg(self, video_filename):
        """Extracts audio from video using ffmpeg-python and saves it as a WAV file."""
        base, _ = os.path.splitext(video_filename)
        audio_filename = f"{base}.wav"
        try:
            # Using ffmpeg-python to extract audio from video
            (
                ffmpeg
                .input(video_filename)
                .output(audio_filename, acodec='pcm_s16le', ar='16000', ac=1)  # 16kHz mono, WAV format
                .run(overwrite_output=True)
            )
            logger.info(f"Audio extracted to {audio_filename}")
            return audio_filename
        except ffmpeg.Error as e:
            logger.error(f"Error extracting audio from {video_filename}: {e.stderr.decode()}", exc_info=True)
            return None

    def generate_subtitles(self, audio_file, media_filename):
        """Generates subtitles from audio using Whisper."""
        if not audio_file:
            logger.error("No audio file provided for subtitle generation.")
            return

        logger.info(f"Generating subtitles from audio: {audio_file} using Whisper")
        try:
            # Load Whisper model
            result = self.model.transcribe(audio_file)

            transcript = result.get('text', '')
            if transcript:
                # Check for keywords in the transcript
                if self.contains_keywords(transcript):
                    # Save the transcript as subtitles
                    self.save_whisper_subtitles_with_timestamps(result, audio_file)
                else:
                    # If keywords are not found, delete video/audio files
                    logger.info(f"Keywords not found in transcript. Deleting {media_filename} and associated files.")
                    self.cleanup_files(media_filename)

        except Exception as e:
            logger.error(f"Error generating subtitles for {audio_file}: {e}", exc_info=True)

    def save_whisper_subtitles_with_timestamps(self, result, audio_file):
        """Saves the Whisper transcript with timestamps as an SRT subtitle file."""
        srt_file = audio_file.replace(".wav", ".srt")
        try:
            # Parse the result to get segment-level timestamps
            segments = result.get('segments', [])
            if not segments:
                logger.warning("No segments found in the Whisper transcription result.")
                return

            # Open SRT file for writing
            with open(srt_file, "w", encoding='utf-8') as f:
                for idx, segment in enumerate(segments, 1):
                    start_time = timedelta(seconds=segment['start'])
                    end_time = timedelta(seconds=segment['end'])
                    text = segment['text']

                    # Format the timestamps
                    start_timestamp = str(start_time) + ",000"
                    end_timestamp = str(end_time) + ",000"

                    # Write the subtitle block
                    f.write(f"{idx}\n")
                    f.write(f"{start_timestamp} --> {end_timestamp}\n")
                    f.write(f"{text.strip()}\n\n")

            logger.info(f"Generated subtitles saved as {srt_file}")
        except Exception as e:
            logger.error(f"Error saving subtitles to {srt_file}: {e}", exc_info=True)
        
    def check_subtitle_keywords(self, subtitle_file):
        """Check for keywords in external subtitle files."""
        logger.info(f"Checking keywords in subtitle file: {subtitle_file}")
        try:
            with open(subtitle_file, 'r', encoding='utf-8') as f:
                content = f.read().lower()
                if self.contains_keywords(content):
                    logger.info(f"Keywords found in subtitle file: {subtitle_file}")
                else:
                    logger.info(f"No keywords found in subtitle file: {subtitle_file}")
                    self.cleanup_files(subtitle_file)
        except Exception as e:
            logger.error(f"Error checking keywords in subtitle file: {subtitle_file} - {e}", exc_info=True)

    def check_embedded_subtitle_keywords(self, video_filename):
        """Check for keywords in embedded subtitles within the MP4 file."""
        logger.info(f"Checking embedded subtitles for keywords in: {video_filename}")
        try:
            # Extract embedded subtitles using pymediainfo
            media_info = MediaInfo.parse(video_filename)
            for track in media_info.tracks:
                if track.track_type == "Text":
                    subtitle_text = track.to_data().get("text", "").lower()
                    if self.contains_keywords(subtitle_text):
                        logger.info(f"Keywords found in embedded subtitles of {video_filename}")
                    else:
                        logger.info(f"No keywords found in embedded subtitles of {video_filename}")
                        self.cleanup_files(video_filename)
        except Exception as e:
            logger.error(f"Error checking embedded subtitles in {video_filename}: {e}", exc_info=True)

    def contains_keywords(self, text):
        """Check if the transcript or subtitles contain any of the specified keywords."""
        text_lower = text.lower()
        final_score , found_keywords, found_anti_keywords =relative_keywords_score(text_lower)
        return final_score > 0

    def save_subtitles_with_timestamps(self, result, audio_file):
        """Saves the transcript with timestamps as an SRT subtitle file."""
        srt_file = audio_file.replace(".wav", ".srt")
        try:
            # Parse the result to get word-level timestamps
            words = result.get('result', [])
            if not words:
                logger.warning("No words found in the recognition result.")
                return

            # Open SRT file for writing
            with open(srt_file, "w", encoding='utf-8') as f:
                for idx, word_info in enumerate(words, 1):
                    start_time = timedelta(seconds=word_info['start'])
                    end_time = timedelta(seconds=word_info['end'])
                    text = word_info['word']

                    # Format the timestamps
                    start_timestamp = str(start_time) + ",000"
                    end_timestamp = str(end_time) + ",000"

                    # Write the subtitle block
                    f.write(f"{idx}\n")
                    f.write(f"{start_timestamp} --> {end_timestamp}\n")
                    f.write(f"{text}\n\n")

            logger.info(f"Generated subtitles saved as {srt_file}")
        except Exception as e:
            logger.error(f"Error saving subtitles to {srt_file}: {e}", exc_info=True)

    def cleanup_files(self, base_filename):
        """Cleans up all related files, including chunks and subtitles."""
        if base_filename:
            base, _ = os.path.splitext(base_filename)
            related_files = glob.glob(f"{base}*")  # Match any related files with the same base name
            for file in related_files:
                try:
                    if os.path.exists(file):
                        os.remove(file)
                        logger.debug(f"Deleted {file}")
                except OSError as e:
                    logger.error(f"Error deleting {file}: {e}", exc_info=True)

    async def run_in_thread(self, func, *args):
        """Runs a blocking function in a separate thread."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self.executor, func, *args)

    def on_finish(self, crawler):
        """Called when the crawler finishes."""
        logger.info("BBCVideoOnDemand plugin finished")
        self.executor.shutdown(wait=True)  # Ensure all threads have completed


# Test code
class MockCrawler:
    def __init__(self):
        pass


async def test_plugin():
        try:
            # Instantiate the plugin
            plugin = BBCVideoOnDemand()

            # Create a mock crawler with sample keywords
            mock_crawler = MockCrawler()

            # Call the on_start method
            plugin.on_start(mock_crawler)

            # Test URLs (replace with actual BBC video/audio URLs for real testing)
            test_urls = [
                "https://www.bbc.co.uk/iplayer/episode/p08yr4dk/lily-a-transgender-story",
                "https://www.bbc.co.uk/sounds/play/p0c5s642"
            ]

            # Simulate the crawling process
            for url in test_urls:
                should_continue = await plugin.before_fetch(url, mock_crawler)
                if not should_continue:
                    logger.info(f"Plugin handled the URL: {url}")
                else:
                    logger.info(f"Plugin did not handle the URL: {url}")

            # Call the on_finish method
            plugin.on_finish(mock_crawler)
        except Exception as e:
            logger.error(f"An error occurred during testing: {e}", exc_info=True)
