from concurrent.futures import ThreadPoolExecutor
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api._errors import NoTranscriptFound, VideoUnavailable, TranscriptsDisabled, IpBlocked
from tenacity import retry, wait_fixed, retry_if_exception_type, stop_after_attempt
from app.types.youtube import Snippet, FetchAndMetaResponse
from app.lib.timeout import TRANSCRIPT_FETCH_TIMEOUT
from app.lib.rd import r
from app.lib.defenses.headers import get_realistic_headers
from typing import Literal
import asyncio
import httpx
import time

headers = get_realistic_headers()
httpx_client = httpx.Client(timeout=TRANSCRIPT_FETCH_TIMEOUT, headers=headers)

# Global API and thread pool
executor = ThreadPoolExecutor(max_workers=30)

def apply_progress(progress_key: str, max_results: int) -> int:
    current = r.incr(progress_key, 1)
    percentage = int((current / max_results) * 100)
    r.set(f"{progress_key}:percentage", percentage)

    return percentage

def apply_status(progress_id: str, status: Literal['done', 'failed']) -> None:
    r.incr(f"status:{progress_id}:{status}", 1)

@retry(
    retry=retry_if_exception_type(IpBlocked),
    wait=wait_fixed(1),
    stop=stop_after_attempt(2)
)
def fetch_transcript_with_snippet(video_id: str, snippet: Snippet, progress_id: str, max_results: int) -> dict | None:
    try:
        ytt_api = YouTubeTranscriptApi(http_client=httpx_client)
        transcript = ytt_api.fetch(video_id).to_raw_data()

        # Increment progress
        progress_key = f"progress:{progress_id}"

        percentage = apply_progress(progress_key, max_results)
        apply_status(progress_id, 'done')

        print(f"✅ {video_id} done, progress: {percentage}%")
        return {
            "video_id": video_id,
            "transcript": transcript,
            "snippet": snippet.model_dump()
        }
    except (NoTranscriptFound, VideoUnavailable, TranscriptsDisabled):
        apply_progress(f"progress:{progress_id}", max_results)
        apply_status(progress_id, 'failed')
        return None
    except Exception as e:
        apply_progress(f"progress:{progress_id}", max_results)
        apply_status(progress_id, 'failed')
        print(f"⚠️ Unexpected error: {e}")
        return None

async def fetch_all_transcripts_with_metadata(video_ids: list[str], snippets: list[Snippet], progress_id: str) -> list[FetchAndMetaResponse]:
    start = time.perf_counter()

    # Set progress logic
    r.set(f"progress:{progress_id}", 0)
    max_results = len(video_ids)

    loop = asyncio.get_running_loop()

    async def run_in_thread(vid, snip):
        return await loop.run_in_executor(executor, fetch_transcript_with_snippet, vid, snip, progress_id, max_results)

    tasks = [run_in_thread(vid, snip) for vid, snip in zip(video_ids, snippets)]
    results = await asyncio.gather(*tasks)
    end = time.perf_counter()

    print(f"Took {end-start} seconds to fetch all transcripts.")
    return [
        FetchAndMetaResponse(
            video_id=result["video_id"],
            transcript=result["transcript"],
            snippet=Snippet(**result["snippet"])
        )
        for result in results if result
    ]