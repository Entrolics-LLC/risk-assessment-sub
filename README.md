# RSP-pose-detection-sub

Minimal Pub/Sub subscriber that forwards video processing requests to an external video analysis API.

Files:
- `main.py` - subscriber and request forwarder
- `requirements.txt` - Python dependencies

Environment variables (see `.env.example`):
- `project_id` - GCP project id
- `video_subscription_id` - Pub/Sub subscription id
- `batch_size` - number of messages to pull concurrently (default 1)
- `VIDEO_ANALYSIS_API` - remote video analysis endpoint

Running locally:
1. Install dependencies: `pip install -r requirements.txt`
2. Set environment variables or create a `.env` file
3. Run: `python main.py`

Notes:
- This project assumes Google application default credentials are available for Pub/Sub and GCS clients.
- The `requests` call currently disables SSL verification (`verify=False`). This is unsafe for production.
