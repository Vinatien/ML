"""
AWS Lambda function to trigger Airflow DAGs.
This can be deployed to AWS Lambda and triggered by:
- CloudWatch Events (schedule)
- EventBridge
- Manual invocation
"""

import json
import os
from datetime import datetime
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def trigger_airflow_dag(dag_id: str, conf: dict = None) -> dict:
    """
    Trigger an Airflow DAG via REST API.
    
    Args:
        dag_id: The ID of the DAG to trigger
        conf: Optional configuration dictionary to pass to the DAG
        
    Returns:
        Response from Airflow API
    """
    # Airflow connection details - load from environment variables
    airflow_url = os.getenv('AIRFLOW_URL')
    airflow_username = os.getenv('AIRFLOW_USERNAME')
    airflow_password = os.getenv('AIRFLOW_PASSWORD')
    
    # API endpoint
    url = f"{airflow_url}/api/v1/dags/{dag_id}/dagRuns"
    
    # Request payload
    payload = {
        "conf": conf or {},
        "dag_run_id": f"lambda_trigger_{datetime.now().isoformat()}",
        "logical_date": datetime.now().isoformat(),
    }
    
    # Make request
    response = requests.post(
        url,
        json=payload,
        auth=(airflow_username, airflow_password),
        headers={'Content-Type': 'application/json'}
    )
    
    return {
        'statusCode': response.status_code,
        'body': response.json() if response.ok else response.text
    }


def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    
    Event structure:
    {
        "dag_id": "postgresql_to_feature_store_etl",
        "conf": {
            "custom_param": "value"
        }
    }
    """
    print(f"Lambda triggered at: {datetime.now().isoformat()}")
    print(f"Event: {json.dumps(event)}")
    
    # Extract DAG ID from event
    dag_id = event.get('dag_id', 'postgresql_to_feature_store_etl')
    dag_conf = event.get('conf', {})
    
    try:
        # Trigger Airflow DAG
        result = trigger_airflow_dag(dag_id, dag_conf)
        
        print(f"DAG triggered successfully: {dag_id}")
        print(f"Response: {json.dumps(result)}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Successfully triggered DAG: {dag_id}',
                'dag_id': dag_id,
                'trigger_time': datetime.now().isoformat(),
                'airflow_response': result
            })
        }
        
    except Exception as e:
        print(f"Error triggering DAG: {str(e)}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Failed to trigger DAG: {dag_id}',
                'error': str(e),
                'trigger_time': datetime.now().isoformat()
            })
        }


# For local testing
if __name__ == "__main__":
    # Test event
    test_event = {
        "dag_id": "postgresql_to_feature_store_etl",
        "conf": {
            "test_run": True
        }
    }
    
    # Mock context
    class MockContext:
        function_name = "test_function"
        memory_limit_in_mb = 128
        invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test"
        aws_request_id = "test-request-id"
    
    # Test the lambda handler
    result = lambda_handler(test_event, MockContext())
    print(json.dumps(result, indent=2))
