from pathlib import Path
import subprocess

from openai import OpenAI


def main() -> None:
    client = OpenAI()

    ticker = "L H A I"
    alert_text = f"We found a signal on {ticker}."

    speech_file = Path("/tmp/scanner_alert.wav")

    with client.audio.speech.with_streaming_response.create(
        model="gpt-4o-mini-tts",
        voice="marin",
        input=alert_text,
        instructions=(
            "Speak clearly and naturally, like a calm trading desk alert. "
            "Slight urgency, but not dramatic."
        ),
        response_format="wav",
    ) as response:
        response.stream_to_file(speech_file)

    subprocess.run(
        ["ffplay", "-nodisp", "-autoexit", "-loglevel", "quiet", str(speech_file)],
        check=True,
    )


if __name__ == "__main__":
    main()