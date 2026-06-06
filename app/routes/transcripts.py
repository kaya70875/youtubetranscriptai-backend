from fastapi import APIRouter, Depends
from fastapi import Query, HTTPException
from fastapi.responses import StreamingResponse
from app.utils.writes import write_as_csv, write_as_text, write_as_json
from app.user.limits import check_request_limits
from app.user.utils import get_user_plan
from app.user.user_limits import USER_LIMITS
from app.user.extract_jwt_token import get_user_id
from app.utils.jobs import get_job_from_redis
from app.lib.redis_settings import REDIS_CONF
from typing import Annotated
from arq import create_pool
from app.types.youtube import FetchAndMetaResponse
from app.lib.rd import r
from app.utils.helpers import get_channel_id
import asyncio
import logging
import uuid
import os
import json

logger = logging.getLogger(__name__)
API_KEY = os.getenv("YOUTUBE_API_KEY")

if not API_KEY:
    raise ValueError("API key not found. Please set the YOUTUBE_API_KEY environment variable.")

router = APIRouter()

@router.get("/transcripts/download/{job_id}")
async def download(job_id: str):
    try:
        # Get cached queris for job
        job = get_job_from_redis(job_id)

        export_type = job.get("export_type")
        include_timing = job.get("include_timing")

        # Get cached transcripts from redis
        cached_cleaned_data = job.get("results")
        
        # Convert metadata into a list.
        allowed_metadata = job.get("allowed_metadata")
        metadata = allowed_metadata.split(",")

        cleaned_data: list[FetchAndMetaResponse] = [FetchAndMetaResponse.model_validate(item) for item in cached_cleaned_data]

        loop = asyncio.get_running_loop()

        if export_type == "txt":
            output = await loop.run_in_executor(
                None, write_as_text, cleaned_data, metadata, include_timing
            )
            return StreamingResponse(output, media_type="text/txt", headers={
                "Content-Disposition": "attachment; filename=transcripts.txt",
            })

        elif export_type == 'csv':
            output = await loop.run_in_executor(
                None, write_as_csv, cleaned_data, metadata, include_timing
            )
            return StreamingResponse(output, media_type="text/csv", headers={
                "Content-Disposition": "attachment; filename=transcripts.csv",
            })

        elif export_type == 'json':
            output = await loop.run_in_executor(
                None, write_as_json, cleaned_data, metadata, include_timing
            )
            return StreamingResponse(output, media_type="application/json", headers={
                "Content-Disposition": "attachment; filename=transcripts.json",
            })
    except Exception as e:
        logger.error('Error while downloading file', e)
        raise HTTPException(status_code=500, detail=f'error: {e}')

@router.get("/transcripts/background/{channel_name}")
async def start_background_fetching_job(
    user_id: Annotated[str, Depends(get_user_id)],
    channel_name: str,
    max_results: int = Query(None),
    export_type: str = Query(default="json", description="Export type for transcripts. Options: 'json', 'txt', 'csv'"),
    allowed_metadata: str = Query(default='title', description="Allowed metadata values. Options : 'title | description | publishedAt'"),
    include_timing: bool = Query(default=True, description="Whether the include start and duration parameters or not.")
):
    
    if export_type not in ["json", "txt", "csv"]:
        raise HTTPException(status_code=400, detail="Invalid export type. Options: 'json', 'txt', 'csv'")
    
    user_plan = await get_user_plan(user_id)
    user_plan = user_plan.lower().replace(' ', '_')
    user_limits = USER_LIMITS.get(user_plan, USER_LIMITS['free'])
    max_allowed = int(user_limits['max_videos'])

    if max_results is None:
        max_results = max_allowed
    if max_results > max_allowed:
        logger.error("Limit exceeds user plan")
        return {"error": f"Max {max_allowed} videos allowed for {user_plan} plan."}

    await check_request_limits(user_id, user_limits=user_limits)

    channel_id = await get_channel_id(channel_name, API_KEY)
    if not channel_id:
        raise HTTPException(status_code=404, detail=f"Channel '{channel_name}' not found.")

    arq = await create_pool(REDIS_CONF)

    # Set a progress id for tracking progress real time with server sent events.
    progress_id = str(uuid.uuid4())

    job = await arq.enqueue_job(
        "fetch_transcripts_task",
        progress_id,
        channel_id,
        max_results,
        user_id,
    )

    # Get job id
    job_id = job.job_id

    # Set query for jobs route
    r.hset(f'query:{job_id}', mapping={
        "channel_name": channel_name,
        "max_results": max_results,
        "export_type": export_type,
        "allowed_metadata": allowed_metadata,
        "include_timing": str(include_timing)
    })

    # Set queue info for JobStatus component
    queue_info = {
        "job_id": job_id,
        "progress_id": progress_id,
        "channel_name": channel_name
    }

    key = f"user:{user_id}:in-queue"
    r.sadd(key, json.dumps(queue_info))

    r.expire(f'query:{job_id}', 60 * 60 * 2) # 2 hours of expiry time

    return {"job_id": job_id, "progress_id": progress_id}