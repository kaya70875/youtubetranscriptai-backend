# YouTubeTranscriptAI Backend

FastAPI-based backend for extracting YouTube transcripts at scale, processing them asynchronously, and exporting structured datasets for machine learning and NLP workflows.

## What it does

- Extracts transcripts from public YouTube videos
- Processes large channels asynchronously
- Supports long-running jobs with progress tracking
- Caches repeated work for better performance
- Exports data in structured formats such as CSV, JSON, and TXT

## Tech Stack

- **Backend:** FastAPI, Python
- **Async / Jobs:** Celery, Redis
- **Database:** PostgreSQL
- **Data handling:** Pydantic
- **Deployment / ops:** Docker

## Features

- Asynchronous transcript extraction
- Channel-wide batch processing
- Background job execution
- Progress-aware workflow for long-running tasks
- Structured export pipeline for downstream analysis
- API-first architecture with Swagger/OpenAPI support

## Project Structure

```text
app/
  api/
  core/
  services/
  models/
  workers/
```

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/kaya70875/youtubetranscriptai-backend.git
cd youtubetranscriptai-backend
```

### 2. Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate   # macOS/Linux
# .venv\Scripts\activate    # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

Create a `.env` file and set the values required by your app, for example:

```env
DATABASE_URL=postgresql://user:password@localhost:5432/youtubetranscriptai
REDIS_URL=redis://localhost:6379/0
```

If your project uses additional keys for YouTube access, worker settings, or storage, add them here as well.

### 5. Run the app

```bash
uvicorn main:app --reload
```

### 6. Run the worker

```bash
celery -A worker.celery worker --loglevel=info
```

## API

Once the app is running, open:

- Swagger UI: `/docs`
- ReDoc: `/redoc`

## Example Use Case

This backend is useful for:

- building training datasets from YouTube content
- transcript collection at scale
- NLP preprocessing pipelines
- internal research tools
- transcript-based analytics

## Notes

- This project is designed around long-running extraction jobs, so background workers matter.
- Adjust the module names in the run commands if your app entry points differ.
- If you extend the system, keep the API responses stable and the export formats predictable.

## License

Add a license if you plan to make the project easier to reuse publicly.
