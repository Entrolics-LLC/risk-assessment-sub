import requests
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import FlowControl
# from prometheus_client import start_http_server
import logging
logging.basicConfig(level=logging.DEBUG)
from dotenv import load_dotenv
from google.cloud import storage
import json 
import os 
from pydantic import BaseModel
import traceback
from prometheus_client import start_http_server
from google.cloud import pubsub_v1


# Start Prometheus metrics server
start_http_server(8000)

# Load environment variables
load_dotenv()


# Configuration for Pub/Sub
project_id = os.getenv("project_id")
subscription_id = os.getenv("risk_subscription_id")  # Changed from pdf_subscription_id
NUM_MESSAGES = int(os.getenv("batch_size",1))

# Risk assessment API endpoint
RISK_ASSESSMENT_API = os.getenv("RISK_ASSESSMENT_API", "http://127.0.0.1:8001//api/classify_document")  # Changed from pdf_subscription_id

# video metadata analysis topic
RISK_ASSESSMENT_TOPIC = os.getenv("RISK_ASSESSMENT_TOPIC", "risk-assessment-sub")



subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# Initialize Pub/Sub publisher
publisher = pubsub_v1.PublisherClient()

# Initialize GCS client
storage_client = storage.Client()



class RiskAssessmentRequest(BaseModel):
    company_id: str
    document_id: str
    gs_uri: str  # e.g., gs://training-workout-bucket/demo3.mp4



def process_risk_assessment(request: RiskAssessmentRequest, transaction_id: str = "unknown"):
    """
    Process risk assessment by calling the risk assessment API
    """
    company_id = request.company_id
    document_id = request.document_id
    gs_uri = request.gs_uri
    
    try:
        print(f"[{transaction_id}] Preparing to call risk assessment API for {gs_uri}")

        # Prepare the payload for the risk assessment API
        payload = {
            "company_id": company_id,
            "document_id": document_id,
            "gs_uri": gs_uri
        }

        print(f"[{transaction_id}] Calling risk assessment API with payload: {payload}")

        # Make the API call
        response = requests.post(
            RISK_ASSESSMENT_API,
            headers={
                'accept': 'application/json',
                'Content-Type': 'application/json'
            },
            json=payload,
            timeout=(30, 1200), # 5 minute timeout for video processing
            # allow_redirects=False,
            verify=False # Temporary SSL verification bypass
        )

        if response.status_code in [301, 302, 307, 308]:
            print(f"Redirect detected! Status: {response.status_code}, Location: {response.headers.get('Location')}")

        # Raise an exception for bad status codes
        response.raise_for_status()

        print(f"[{transaction_id}] Risk assessment API call completed successfully. Response: {response.status_code}")

        return response.json()
        
    except requests.exceptions.Timeout as e:
        print(f"[{transaction_id}] Video analysis API call timed out")
        error_details = f"API Timeout Error: {str(e)}\n\nStacktrace:\n{traceback.format_exc()}"
        raise
    except requests.exceptions.RequestException as e:
        print(f"[{transaction_id}] Video analysis API call failed: {str(e)}")
        error_details = f"API Request Error: {str(e)}\n\nStacktrace:\n{traceback.format_exc()}"
        raise
    except Exception as e:
        print(f"[{transaction_id}] Unexpected error in risk assessment processing: {str(e)}")
        error_details = f"Unexpected Processing Error: {str(e)}\n\nStacktrace:\n{traceback.format_exc()}"
        raise

# Callback function that processes incoming Pub/Sub messages
def callback(message: pubsub_v1.subscriber.message.Message):
    video_id = None

    try:
        msg = message.data.decode('utf-8')
        json_data = json.loads(msg)

        if msg:
            print(f"Received message: {msg}")
        else:
            print("Received empty message")
            message.nack()
            return

        # Extract video-specific parameters
        company_id = json_data.get('company_id')
        document_id = json_data.get('document_id')
        gs_uri = json_data.get('gs_uri')

        no_of_retry = message.delivery_attempt
        transaction_id = document_id or 'unknown'

        print(
            f"[{transaction_id}] Video Processing Parameters:\n"
            f"company_id: {company_id}\n"
            f"document_id: {document_id}\n"
            f"gs_uri: {gs_uri}\n"
            f"No_of_Retry: {no_of_retry}"
        )

        # Validate required fields
        if not company_id:
            raise ValueError("company_id is required")
        if not document_id:
            raise ValueError("document_id is required")
        if not gs_uri:
            raise ValueError("gs_uri is required")

        # -----------------------------
        # STEP 1: Process full video analysis
        # -----------------------------
        print(f"[{transaction_id}] Starting risk assessment for {gs_uri}...")
        analysis_result = process_risk_assessment(
            request=RiskAssessmentRequest(gs_uri=gs_uri, document_id=document_id),
            transaction_id=transaction_id
        )
        if analysis_result:
            print(f"[{transaction_id}] ✅ Risk assessment completed successfully")
        else:
            raise ValueError(f"[{transaction_id}] ❌ Failed to get result")

        # Ack the message only if both analyses succeeded
        message.ack()
        print(f"[{transaction_id}] ✅ All analyses completed — message acked")

    except json.JSONDecodeError as e:
        error_trace = traceback.format_exc()
        transaction_id = video_id or 'unknown'
        print(f"[{transaction_id}] ❌ Invalid JSON in message: {e}\n{error_trace}")
        message.nack()

    except ValueError as e:
        error_trace = traceback.format_exc()
        transaction_id = video_id or 'unknown'
        print(f"[{transaction_id}] ❌ Missing required field: {e}\n{error_trace}")
        message.nack()

    except requests.exceptions.Timeout as e:
        error_trace = traceback.format_exc()
        transaction_id = video_id or 'unknown'
        print(f"[{transaction_id}] ❌ API call timed out for {video_id}\n{error_trace}")
        message.nack()

    except Exception as e:
        error_trace = traceback.format_exc()
        transaction_id = video_id or 'unknown'
        print(f"[{transaction_id}] ❌ Error processing video {video_id}: {e}\n{error_trace}")
        message.nack()


if __name__ == "__main__":
    # Configure flow control settings
    flow_control = FlowControl(
        max_bytes=104857600,
        max_messages=NUM_MESSAGES,
        max_lease_duration=30*60  # 30 minutes for video processing
    )
        
    # Subscribe to the subscription and pass the callback
    future = subscriber.subscribe(subscription_path, callback=callback, flow_control=flow_control)
    print(f"Subscribed to {subscription_path} with flow control: {flow_control}")

    # Keep the main thread alive while messages are being processed
    print(f"Listening for process risk assessment messages on {subscription_path}...")
    try:
        future.result()  # Blocks the main thread indefinitely, receiving messages
    except KeyboardInterrupt:
        future.cancel()  # Cancel the subscription when interrupted
        print("Process risk assessment subscription canceled.")
