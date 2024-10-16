import os
import glob
import logging
import re
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlparse
import xml.etree.ElementTree as ET
import yt_dlp
from pymediainfo import MediaInfo
import ffmpeg
import aiofiles
import whisper
from utils.keywords import relative_keywords_score
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
        self.process_executor = ProcessPoolExecutor(max_workers=os.cpu_count())  # For CPU-bound tasks
        self.model = whisper.load_model("tiny")  # Use "tiny" for faster transcription

    async def on_start(self, crawler):
        logger.info("BBCVideoOnDemand plugin started")

    async def before_fetch(self, url, crawler):
        parsed_url = urlparse(url)
        if "bbc.co.uk" in parsed_url.netloc:
            path_parts = parsed_url.path.strip("/").split("/")
            if len(path_parts) >= 3 and path_parts[0] == "iplayer" and path_parts[1] == "episode":
                episode_id = path_parts[2]
                logger.info(f"BBC iPlayer episode detected: {episode_id} from URL: {url}")
                await self.process_video_async(url)
                return False
            elif len(path_parts) >= 2 and path_parts[0] == "sounds" and path_parts[1] == "play":
                sound_id = path_parts[2] if len(path_parts) > 2 else None
                logger.info(f"BBC Sounds play page detected: {sound_id} from URL: {url}")
                await self.process_audio_async(url)
                return False
        return True

    async def process_video_async(self, url):
        video_filename = await self.run_in_thread(self.download_video, url)
        if video_filename:
            subtitle_file = self.find_subtitle_file(video_filename)
            if subtitle_file:
                await self.run_in_thread(self.check_subtitle_keywords, subtitle_file)
            else:
                audio_file = await self.run_in_thread(self.extract_audio_ffmpeg, video_filename)
                if audio_file:
                    await self.transcribe_audio_async(audio_file)

    async def process_audio_async(self, url):
        audio_filename = await self.run_in_thread(self.download_audio, url)
        if audio_filename:
            await self.transcribe_audio_async(audio_filename)

    async def transcribe_audio_async(self, audio_file):
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(self.process_executor, self.model.transcribe, audio_file)
        transcript = result.get('text', '')
        if transcript and self.contains_keywords(transcript):
            await self.run_in_thread(self.save_whisper_subtitles_with_timestamps, result, audio_file)
        else:
            logger.info(f"Keywords not found in {audio_file}. Deleting the file.")
            self.cleanup_files(audio_file)

    def download_video(self, url):
        logger.info(f"Downloading video from {url} using yt-dlp API...")
        try:
            ydl_opts = {
                'format': 'worst',
                'outtmpl': f'{self.media_dir}/%(title)s.%(ext)s',
                'writesubtitles': True,
                'subtitleslangs': ['en'],
                'quiet': True,
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info_dict = ydl.extract_info(url, download=True)
                return ydl.prepare_filename(info_dict)
        except Exception as e:
            logger.error(f"Error downloading video from {url}: {e}", exc_info=True)
            return None

    def download_audio(self, url):
        logger.info(f"Downloading audio from {url} using yt-dlp API...")
        try:
            ydl_opts = {
                'format': 'bestaudio/best',
                'outtmpl': f'{self.media_dir}/%(title)s.%(ext)s',
                'quiet': True,
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info_dict = ydl.extract_info(url, download=True)
                return ydl.prepare_filename(info_dict)
        except Exception as e:
            logger.error(f"Error downloading audio from {url}: {e}", exc_info=True)
            return None

    def extract_audio_ffmpeg(self, video_filename):
        base, _ = os.path.splitext(video_filename)
        audio_filename = f"{base}.wav"
        try:
            ffmpeg.input(video_filename).output(audio_filename, acodec='pcm_s16le', ar='16000', ac=1).run(overwrite_output=True)
            logger.info(f"Audio extracted to {audio_filename}")
            return audio_filename
        except ffmpeg.Error as e:
            logger.error(f"Error extracting audio from {video_filename}: {e.stderr.decode()}", exc_info=True)
            return None

    async def process_ttml_subtitles(self, ttml_file):
        output_srt = ttml_file.replace(".en.ttml", ".srt")
        try:
            # Regex pattern to match <p> elements in TTML
            ttml_pattern = re.compile(r'<p begin="(?P<start>[^"]+)" end="(?P<end>[^"]+)">(?P<text>.*?)</p>', re.DOTALL)

            async with aiofiles.open(ttml_file, 'r', encoding='utf-8') as ttml_fp:
                async with aiofiles.open(output_srt, 'w', encoding='utf-8') as srt_fp:
                    content = await ttml_fp.read()
                    matches = ttml_pattern.finditer(content)
                    count = 1

                    # Process each <p> element and convert to SRT format
                    for match in matches:
                        start_time = match.group('start')
                        end_time = match.group('end')
                        text = re.sub(r'<[^>]+>', '', match.group('text').strip())  # Remove inner tags if any

                        # Write the subtitle in SRT format
                        await srt_fp.write(f"{count}\n")
                        await srt_fp.write(f"{start_time} --> {end_time}\n")
                        await srt_fp.write(f"{text}\n\n")
                        count += 1

            return output_srt
        except Exception as e:
            logger.error(f"Error processing TTML file: {e}", exc_info=True)
            return None


    def contains_keywords(self, text):
        final_score, _, _ = relative_keywords_score(text.lower())
        return final_score > 0

    def cleanup_files(self, base_filename):
        if base_filename:
            base, _ = os.path.splitext(base_filename)
            related_files = glob.glob(f"{base}*")
            for file in related_files:
                try:
                    os.remove(file)
                except OSError as e:
                    logger.error(f"Error deleting {file}: {e}", exc_info=True)

    async def run_in_thread(self, func, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, func, *args)

    def on_finish(self, crawler):
        logger.info("BBCVideoOnDemand plugin finished")
        self.executor.shutdown(wait=True)
        self.process_executor.shutdown(wait=True)


# Test code
class MockCrawler:
    def __init__(self):
        pass

async def test_plugin():
    try:
        plugin = BBCVideoOnDemand()
        mock_crawler = MockCrawler()
        plugin.on_start(mock_crawler)
        test_urls = [
            "https://www.bbc.co.uk/iplayer/episode/p08yr4dk/lily-a-transgender-story",
            "https://www.bbc.co.uk/sounds/play/p0c5s642"
        ]
        for url in test_urls:
            should_continue = await plugin.before_fetch(url, mock_crawler)
            if not should_continue:
                logger.info(f"Plugin handled the URL: {url}")
            else:
                logger.info(f"Plugin did not handle the URL: {url}")
        plugin.on_finish(mock_crawler)
    except Exception as e:
        logger.error(f"An error occurred during testing: {e}", exc_info=True)
