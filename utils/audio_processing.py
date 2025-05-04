import os
import librosa
import numpy as np
import soundfile as sf
import noisereduce as nr
from scipy.signal import butter, lfilter
import webrtcvad
import logging


output_folder = os.getenv("LOCAL_OUTPUT_FOLDER", "/tmp/processed_audio")
os.makedirs(output_folder, exist_ok=True)


def process_audio(file_path):
    """
    Loads an audio file, applies noise reduction, reverberation removal, and VAD segmentation.
    Saves the processed audio to a specified directory.
    """
    try:
        logging.info(f"Processing file: {file_path}")
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Audio file '{file_path}' not found.")

        y, sr = librosa.load(file_path, sr=None, mono=True)

        # Ensure sample rate compatibility for VAD
        if sr not in [8000, 16000, 32000, 48000]:
            y = librosa.resample(y, orig_sr=sr, target_sr=16000)
            sr = 16000

        processed_chunks = []
        chunk_size = CHUNK_DURATION * sr

        for i in range(0, len(y), chunk_size):
            chunk = y[i:i + chunk_size]

            # Detect speech segments
            segments = vad_segment(chunk, sr, frame_duration_ms=30, aggressiveness=2, padding_duration_ms=300)
            processed_chunk = np.zeros_like(chunk)

            if not segments:
                processed_chunk = normalize_audio(
                    bandpass_filter(
                        reduce_noise(chunk, sr),
                        lowcut=80, highcut=7000, fs=sr
                    )
                )
            else:
                for (start, end) in segments:
                    speech_segment = chunk[start:end]
                    segment_bp = bandpass_filter(
                        normalize_audio(reduce_reverb(reduce_noise(speech_segment, sr))),
                        lowcut=80, highcut=7000, fs=sr
                    )
                    processed_chunk[start:end] = segment_bp

            processed_chunks.append(processed_chunk)

        final_audio = np.concatenate(processed_chunks)
        output_file = os.path.join(output_folder, os.path.basename(file_path))

        # Save processed audio
        # output_path = os.path.join(output_folder, f"{job_id}_processed.wav")
        sf.write(output_file, final_audio, sr)

        logging.info(f"✅ Processed audio saved: {output_file}")
        return output_file

    except Exception as e:
        logging.error(f"Error processing audio file: {str(e)}")
        return None
    
CHUNK_DURATION = 10 * 60  # 10 minutes

def bandpass_filter(data, lowcut, highcut, fs, order=5):
    """
    Applies a bandpass filter that retains frequencies typically associated with the human voice.
    For speech, a band from 80 Hz to 7000 Hz is a good starting point.
    """
    nyq = 0.5 * fs
    low = lowcut / nyq
    high = highcut / nyq
    b, a = butter(order, [low, high], btype='band')
    return lfilter(b, a, data)

def reduce_reverb(y, sr):
    """
    Applies a simple dereverberation method using spectral subtraction.
    This approach computes the STFT of the signal, estimates a reverb profile using
    the median of the magnitude spectrum, subtracts a fraction of it, and then reconstructs the signal.
    """
    # Compute STFT with an appropriate window size
    S = librosa.stft(y, n_fft=1024, hop_length=512)
    magnitude, phase = np.abs(S), np.angle(S)
    # Estimate reverberation as the median across time (a robust estimate)
    reverb_estimate = np.median(magnitude, axis=1, keepdims=True)
    # Subtract a fraction of the reverb estimate (tweak the factor as needed)
    enhanced_magnitude = np.maximum(magnitude - 0.5 * reverb_estimate, 0)
    S_enhanced = enhanced_magnitude * np.exp(1j * phase)
    y_enhanced = librosa.istft(S_enhanced, hop_length=512)
    return y_enhanced

def reduce_noise(y, sr):
    """
    Applies noise reduction using the noisereduce library.
    """
    return nr.reduce_noise(y=y, sr=sr)

def normalize_audio(y):
    """
    Normalizes the audio to ensure consistent amplitude levels (peak normalization).
    """
    return librosa.util.normalize(y)

# --- VAD segmentation functions ---

def float_to_pcm16(audio):
    """
    Converts float audio (in range -1.0 to 1.0) to 16-bit PCM bytes.
    """
    audio_int16 = np.clip(audio * 32767, -32768, 32767).astype(np.int16)
    return audio_int16.tobytes()

def vad_segment(audio, sr, frame_duration_ms=30, aggressiveness=2, padding_duration_ms=300):
    """
    Uses WebRTC VAD to segment the audio into voiced segments.
    
    Parameters:
      audio: 1D numpy array of floats
      sr: Sample rate in Hz (must be one of 8000, 16000, 32000, 48000)
      frame_duration_ms: Duration of each frame (in milliseconds)
      aggressiveness: VAD aggressiveness (0 is least, 3 is most aggressive)
      padding_duration_ms: Duration in ms used to merge short gaps.
    
    Returns:
      A list of tuples (start_sample, end_sample) for each detected speech segment.
    """
    vad = webrtcvad.Vad(aggressiveness)
    frame_samples = int(sr * frame_duration_ms / 1000)
    num_frames = len(audio) // frame_samples
    pcm_data = float_to_pcm16(audio)
    bytes_per_frame = frame_samples * 2  # 2 bytes per sample (16-bit PCM)
    
    # Determine voiced frames
    voiced_flags = []
    for i in range(num_frames):
        start_byte = i * bytes_per_frame
        frame_bytes = pcm_data[start_byte:start_byte + bytes_per_frame]
        is_speech = vad.is_speech(frame_bytes, sr)
        voiced_flags.append(is_speech)
    
    # Group contiguous voiced frames (allowing short silent gaps defined by padding)
    indices = [i for i, flag in enumerate(voiced_flags) if flag]
    if not indices:
        return []
    
    segments = []
    current_start = indices[0]
    current_end = indices[0]
    padding_frames = int(padding_duration_ms / frame_duration_ms)
    
    for idx in indices[1:]:
        if idx - current_end <= padding_frames:
            current_end = idx
        else:
            segments.append((current_start, current_end))
            current_start = idx
            current_end = idx
    segments.append((current_start, current_end))
    
    # Convert frame indices to sample indices and add extra padding (in samples) on each side
    padding_samples = int((padding_duration_ms / 1000) * sr)
    sample_segments = []
    for seg in segments:
        start_sample = max(0, seg[0] * frame_samples - padding_samples)
        end_sample = min(len(audio), (seg[1] + 1) * frame_samples + padding_samples)
        sample_segments.append((start_sample, end_sample))
    
    return sample_segments
