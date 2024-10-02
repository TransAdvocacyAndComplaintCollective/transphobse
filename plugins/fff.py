import yt_dlp
from pydub import AudioSegment
import wave
from vosk import Model, KaldiRecognizer, SetLogLevel
import json

# Set log level to minimize Vosk output
SetLogLevel(0)

def download_video(url):
    try:
        ydl_opts = {
            'format': 'best',
            'outtmpl': '%(title)s.%(ext)s',
            'writesubtitles': True,
            'subtitleslangs': ['en'],
            'quiet': True,
        }

        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info_dict = ydl.extract_info(url, download=True)
            video_filename = ydl.prepare_filename(info_dict)
        
        return video_filename

    except Exception as e:
        print(f"Error downloading video: {e}")
        return None

def extract_audio(video_filename):
    try:
        audio_filename = video_filename.replace(".mp4", ".wav")
        video = AudioSegment.from_file(video_filename, format="mp4")
        video.export(audio_filename, format="wav")
        return audio_filename
    except Exception as e:
        print(f"Error extracting audio: {e}")
        return None

def generate_transcript(audio_filename):
    try:
        wf = wave.open(audio_filename, "rb")
        model = Model(lang="en-us")  # Ensure model is correctly set up
        rec = KaldiRecognizer(model, wf.getframerate())
        rec.SetWords(True)
        
        transcript = []
        while True:
            data = wf.readframes(4096)  # Adjust buffer size
            if len(data) == 0:
                break
            if rec.AcceptWaveform(data):
                result = json.loads(rec.Result())
                transcript.append(result)
            else:
                partial_result = json.loads(rec.PartialResult())
                transcript.append(partial_result)
        
        final_result = json.loads(rec.FinalResult())
        transcript.append(final_result)
        
        return json.dumps(transcript, indent=2)

    except Exception as e:
        print(f"Error generating transcript: {e}")
        return None

# Example usage:
if __name__ == "__main__":
    video_url = "https://www.bbc.co.uk/sounds/play/p03r8pgf"  # Replace with actual BBC URL
    video_filename = download_video(video_url)

    if video_filename:
        audio_filename = extract_audio(video_filename)

        if audio_filename:
            transcript = generate_transcript(audio_filename)
            if transcript:
                print("Generated Transcript:\n", transcript)
