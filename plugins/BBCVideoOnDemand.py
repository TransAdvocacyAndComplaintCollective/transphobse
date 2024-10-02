import os
from pathlib import Path
import yt_dlp
from utils.PluginBase import PluginBase
from pydub import AudioSegment
from vosk import Model, KaldiRecognizer
import json
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import threading
import logging
import glob

# Set up logging
logging.basicConfig(level=logging.INFO)


class BBCVideoOnDemand(PluginBase):
    def __init__(self, max_workers=5):
        self.media_dir = "./media"  # Define the media directory for output files
        self.ensure_media_dir_exists()
        self.lock = threading.Lock()

        # Create a ThreadPoolExecutor for running blocking tasks
        self.executor = ThreadPoolExecutor(max_workers=os.cpu_count())  # Dynamically set max_workers

        # Load Vosk model once during initialization and reuse it
        self.model = self.load_vosk_model()

    def ensure_media_dir_exists(self):
        """Ensure the media directory exists."""
        if not os.path.exists(self.media_dir):
            os.makedirs(self.media_dir)

    def load_vosk_model(self):
        """Load the Vosk model once during initialization."""
        with self.lock:
            # model_path = Path("./model/vosk-model-en-us-0.22").resolve()
            # if not model_path.exists():
                # raise FileNotFoundError(f"Model path {model_path} not found.")
            logging.info("Loading Vosk model...")
            # return Model(str(model_path))  # Load the model once

    def on_start(self, crawler):
        """Called when the crawler starts."""
        logging.info("BBC_VideoOnDemand plugin started")
        self.keywords = crawler.keywords

    async def before_fetch(self, url, crawler):
        """Called before a URL is fetched. Checks if the URL is a BBC video."""
        if "bbc.co.uk" in url:
            if any(substring in url for substring in ["/sounds/play", "/iplayer/episodes"]):
                logging.info(f"BBC_Video plugin found video: {url}")
                
                # Run process_video in a separate thread
                # await self.run_in_thread(self.process_video, url)
                return False  # URL processed and won't be fetched by the crawler
        return True  # URL doesn't match the video patterns, continue crawling

    def process_video(self, url):
        """Handles the full process of downloading, extracting, and generating subtitles."""
        try:
            video_filename = self.download_video(url)
            if video_filename:
                subtitle_file = self.find_subtitle_file(video_filename)
                if subtitle_file:
                    logging.info(f"Subtitles found: {subtitle_file}")
                else:
                    logging.warning("No subtitles found, extracting audio for transcription...")
                    audio_file = self.extract_audio(video_filename)
                    self.generate_subtitles(audio_file, video_filename)
        except Exception as e:
            logging.error(f"Error processing video: {e}")
            # Clean up partial downloads or extracted files
            self.cleanup_files(video_filename)
        finally:
            # Ensure final cleanup
            self.cleanup_files(video_filename)

    def download_video(self, url):
        """Handles downloading video and extracting subtitles."""
        logging.info(f"Downloading video from {url} using yt-dlp API...")
        video_filename = None
        try:
            ydl_opts = {
                'format': 'best',
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
            logging.error(f"Error downloading video: {e}")
            # Clean up in case of error
            self.cleanup_files(video_filename)
            return None

    def find_subtitle_file(self, video_filename):
        """Finds the subtitle file based on the video filename."""
        base, _ = os.path.splitext(video_filename)
        for ext in ['.vtt', '.srt']:
            subtitle_file = f"{base}{ext}"
            if os.path.exists(subtitle_file):
                return subtitle_file
        return None

    def extract_audio(self, video_filename):
        """Extracts audio from video using pydub and saves it as a WAV file."""
        audio_filename = video_filename.replace(".mp4", ".wav")
        try:
            video = AudioSegment.from_file(video_filename, format="mp4")
            video.export(audio_filename, format="wav")
            logging.info(f"Audio extracted to {audio_filename}")
            return audio_filename
        except Exception as e:
            logging.error(f"Error extracting audio from {video_filename}: {e}")
            self.cleanup_files(video_filename, audio_filename)
            return None

    def split_audio(self, audio_file, chunk_size=60_000):
        """Splits audio into smaller chunks of specified length in milliseconds."""
        audio = AudioSegment.from_file(audio_file, format="wav")
        chunks = [audio[i:i + chunk_size] for i in range(0, len(audio), chunk_size)]
        chunk_filenames = []
        for idx, chunk in enumerate(chunks):
            chunk_filename = f"{audio_file.replace('.wav', '')}_chunk{idx}.wav"
            chunk.export(chunk_filename, format="wav")
            chunk_filenames.append(chunk_filename)
        return chunk_filenames

    def preprocess_audio(self, audio_file):
        """Preprocess audio: reduce noise, convert to mono, normalize volume."""
        audio = AudioSegment.from_file(audio_file, format="wav")
        
        # Convert to mono
        audio = audio.set_channels(1)
        
        # Normalize the volume to ensure even levels
        audio = audio.normalize()
        
        # Export the preprocessed audio
        preprocessed_file = audio_file.replace(".wav", "_preprocessed.wav")
        audio.export(preprocessed_file, format="wav")
        return preprocessed_file

    def generate_subtitles(self, audio_file, video_filename):
        """Generates subtitles from audio using Vosk offline speech recognition."""
        logging.info(f"Generating subtitles from audio: {audio_file} {video_filename}")

        try:
            # Split the audio into smaller chunks
            audio_chunks = self.split_audio(audio_file)

            for chunk in audio_chunks:
                preprocessed_audio_file = self.preprocess_audio(chunk)

                recognizer = KaldiRecognizer(self.model, 16000)

                # Adjust beam size to avoid memory usage warnings
                recognizer.SetMaxActive(5000)  # Limit number of active states
                recognizer.SetBeam(4.0)  # Lower beam size for faster, less resource-intensive decoding

                with open(preprocessed_audio_file, "rb") as audio:
                    audio_data = audio.read()
                    recognizer.AcceptWaveform(audio_data)
                    result = json.loads(recognizer.FinalResult())
                    transcript = result['text']
                    logging.info("Transcript: " + transcript)

                    # Add punctuation to the transcript (optional, currently skipped)
                    punctuated_transcript = self.add_punctuation(transcript)

                    # Check for keywords in the transcript
                    if self.contains_keywords(punctuated_transcript):
                        # Save the transcript as subtitles
                        self.save_subtitles(punctuated_transcript, preprocessed_audio_file)
                    else:
                        # If keywords are not found, delete video and audio files
                        logging.info(f"Keywords not found in transcript. Deleting {video_filename} and associated files.")
                        self.cleanup_files(video_filename, preprocessed_audio_file)
        except Exception as e:
            logging.error(f"Error generating subtitles: {e}")
            self.cleanup_files(video_filename, audio_file)

    def contains_keywords(self, transcript):
        """Check if the transcript contains any of the specified keywords."""
        transcript_lower = transcript.lower()
        return any(keyword.lower() in transcript_lower for keyword in self.keywords)

    def delete_files(self, *filenames):
        """Deletes the specified files."""
        for filename in filenames:
            try:
                if filename and os.path.exists(filename):
                    os.remove(filename)
                    logging.info(f"Deleted {filename}")
            except OSError as e:
                logging.error(f"Error deleting {filename}: {e}")

    def cleanup_files(self, base_filename):
        """Cleans up all related files, including chunks and subtitles."""
        if base_filename:
            # Find and delete all files related to the base file (e.g., chunks, subtitles)
            base, _ = os.path.splitext(base_filename)
            related_files = glob.glob(f"{base}*")  # Match any related files with the same base name
            for file in related_files:
                self.delete_files(file)

    def add_punctuation(self, transcript):
        """Optionally add punctuation to the transcript."""
        return transcript  # No punctuation added for now

    def save_subtitles(self, transcript, audio_file):
        """Saves the transcript as an SRT subtitle file with proper timing."""
        srt_file = audio_file.replace(".wav", ".srt")
        
        # Split the transcript into words
        words = transcript.split()

        # Number of words per subtitle block (adjust as needed)
        words_per_block = 10

        # Duration for each subtitle block (adjust based on average speech speed)
        seconds_per_block = 5

        # Open SRT file for writing
        with open(srt_file, "w") as f:
            for i in range(0, len(words), words_per_block):
                # Calculate start and end time for this subtitle block
                start_time = timedelta(seconds=(i // words_per_block) * seconds_per_block)
                end_time = start_time + timedelta(seconds=seconds_per_block)

                # Format the timestamps
                start_timestamp = str(start_time).split(".")[0] + ",000"
                end_timestamp = str(end_time).split(".")[0] + ",000"

                # Write the subtitle block
                f.write(f"{(i // words_per_block) + 1}\n")
                f.write(f"{start_timestamp} --> {end_timestamp}\n")
                f.write(" ".join(words[i:i + words_per_block]) + "\n\n")

        logging.info(f"Generated subtitles saved as {srt_file}")

    async def run_in_thread(self, func, *args):
        """Runs a blocking function in a separate thread."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self.executor, func, *args)

    def on_finish(self, crawler):
        """Called when the crawler finishes."""
        logging.info("BBC_VideoOnDemand plugin finished")
        self.executor.shutdown(wait=True)  # Ensure all threads have completed


main = BBCVideoOnDemand
