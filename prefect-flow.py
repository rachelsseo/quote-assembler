import boto3
import requests
import time
from typing import List, Dict, Any
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect.cache_policies import NONE as NO_CACHE # I was having caching issues for some reason? - so this is for disabling 

# variables
UVA_ID = "ydp7xv"  
AWS_REGION = "us-east-1"
API_URL = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{UVA_ID}"
SUBMIT_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"

# making HTTP POST request 
@task(retries=3, retry_delay_seconds=5, cache_policy=NO_CACHE)
def trigger_message_scatter(uvaid: str) -> Dict[str, Any]:

    logger = get_run_logger()
    api_url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{uvaid}"
    
    logger.info(f"Triggering message scatter for UVA ID: {uvaid}")
    logger.info(f"API URL: {api_url}")
    
    try:
        response = requests.post(api_url)
        response.raise_for_status()
        payload = response.json()
        
        logger.info(f"API Response: {payload}")
        
        sqs_url = payload.get('sqs_url')
        if not sqs_url:
            raise ValueError("No SQS URL returned from API")
        
        logger.info(f"SQS Queue URL: {sqs_url}")
        
        return {
            'status': 'success',
            'sqs_url': sqs_url,
            'expected_messages': 21
        }
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error triggering message scatter: {e}")
        raise

# getting queue attributes to monitor message counts
@task(retries=2, cache_policy=NO_CACHE)
def get_queue_attributes(sqs_client, queue_url: str) -> Dict[str, int]:

    logger = get_run_logger()
    
    try:
        response = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                'ApproximateNumberOfMessages',
                'ApproximateNumberOfMessagesNotVisible',
                'ApproximateNumberOfMessagesDelayed'
            ]
        )
        
        attrs = response['Attributes']
        return {
            'available': int(attrs.get('ApproximateNumberOfMessages', 0)),
            'not_visible': int(attrs.get('ApproximateNumberOfMessagesNotVisible', 0)),
            'delayed': int(attrs.get('ApproximateNumberOfMessagesDelayed', 0))
        }
    except Exception as e:
        logger.warning(f"Error getting queue attributes: {e}")
        # returning safe defaults to allow the flow to continue
        return {
            'available': 0,
            'not_visible': 0,
            'delayed': 0
        }

# receiving messages in batches with long polling 
@task(retries=2, cache_policy=NO_CACHE)
def receive_messages_batch(
    sqs_client,
    queue_url: str,
    max_messages: int = 10,
    wait_time_seconds: int = 20
) -> List[Dict[str, Any]]:

    logger = get_run_logger()
    
    try:
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time_seconds,
            MessageAttributeNames=['All'],
            AttributeNames=['All']
        )
        
        messages = response.get('Messages', [])
        
        if messages:
            logger.info(f"Received {len(messages)} messages in this batch")
        
        return messages
        
    except Exception as e:
        logger.debug(f"Note: {e}")
        return []

# extracting order_no and word from message attributes
@task(cache_policy=NO_CACHE)
def process_message(message: Dict[str, Any]) -> Dict[str, Any]:

    logger = get_run_logger()
    
    try:
        msg_attrs = message.get('MessageAttributes', {})
        
        order_no = msg_attrs.get('order_no', {}).get('StringValue')
        word = msg_attrs.get('word', {}).get('StringValue')
        
        if not order_no or not word:
            raise ValueError(f"Message missing required attributes. Found: order_no={order_no}, word={word}")
        
        # validating the order_no is numeric
        try:
            order_no_int = int(order_no)
        except ValueError:
            raise ValueError(f"order_no must be numeric, got: {order_no}")
        
        processed = {
            'order_no': order_no_int,
            'word': word,
            'receipt_handle': message['ReceiptHandle'],
            'message_id': message['MessageId']
        }
        
        logger.info(f"Processed: order_no={order_no}, word='{word}'")
        
        return processed
        
    except KeyError as e:
        logger.error(f"Missing key in message structure: {e}")
        raise ValueError(f"Invalid message structure: missing {e}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        raise

# deleting messages from the queue
@task(retries=3, retry_delay_seconds=2, cache_policy=NO_CACHE)
def delete_message(sqs_client, queue_url: str, receipt_handle: str, message_id: str) -> bool:

    logger = get_run_logger()
    
    try:
        sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        logger.info(f"Deleted message {message_id}")
        return True
    except Exception as e:
        logger.warning(f"Error deleting message {message_id}: {e}") 
        # not raising exception to allow the flow to continue even if delete fails 
        return False

# assembling the complete phrase!
@task(cache_policy=NO_CACHE)
def assemble_phrase(messages: List[Dict[str, Any]]) -> str:

    logger = get_run_logger()
    logger.info(f"Assembling phrase from {len(messages)} messages...")
    
    # sort by order number
    sorted_messages = sorted(messages, key=lambda x: x['order_no'])
    
    # extracting the words in order
    words = [msg['word'] for msg in sorted_messages]
    
    # joining into a complete phrase
    complete_phrase = ' '.join(words)
    
    logger.info(f"Complete phrase: '{complete_phrase}'")
    logger.info(f"Total words: {len(words)}")
    
    return complete_phrase

# submitting the final answer to the submission queue
@task(retries=3, retry_delay_seconds=5, cache_policy=NO_CACHE)
def submit_answer(
    sqs_client,
    queue_url: str,
    uvaid: str,
    phrase: str,
    platform: str = "prefect"
) -> Dict[str, Any]:
    
    # log details about submission
    logger = get_run_logger()
    logger.info(f"Submitting answer to: {queue_url}")
    logger.info(f"UVA ID: {uvaid}")
    logger.info(f"Platform: {platform}")
    logger.info(f"Phrase: '{phrase}'")
    
    try:
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=f"Solution from {uvaid}",
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': uvaid
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': platform
                }
            }
        )
        
        http_status = response['ResponseMetadata']['HTTPStatusCode']
        message_id = response['MessageId']
        
        logger.info("Submission Response:")
        logger.info(f"HTTP Status Code: {http_status}")
        logger.info(f"Message ID: {message_id}")
        
        if http_status == 200:
            logger.info("Answer submitted successfully!")
        else:
            logger.warning(f"Unexpected response code: {http_status}")
        
        return {
            'status': 'success',
            'http_status': http_status,
            'message_id': message_id,
            'phrase': phrase
        }
        
    except Exception as e:
        logger.error(f"Error submitting answer: {e}")
        raise

# main flow! 
@flow(name="SQS Message Puzzle - Prefect")
def sqs_message_puzzle_flow(
    uvaid: str = UVA_ID,
    aws_region: str = AWS_REGION,
    submit_queue_url: str = SUBMIT_QUEUE_URL,
    expected_count: int = 21,
    max_wait_time: int = 1000,
    poll_interval: int = 10,
    adaptive_polling: bool = True
):

    print("SQS Puzzle - Main Prefect Flow")
    
    # initializing SQS client
    try:
        sqs_client = boto3.client('sqs', region_name=aws_region)
        print(f"SQS client initialized! : {aws_region}")
    except Exception as e:
        print(f"Failed to initialize SQS client: {e}")
        raise ValueError(f"Can't connect to AWS SQS, please check credentials and region. Error: {e}")
    
    # task 1: triggering message scatter
    print("\nTask 1: Triggering message scatter...")
    try:
        trigger_result = trigger_message_scatter(uvaid)
        sqs_url = trigger_result['sqs_url']
    except Exception as e:
        print(f"Failed to trigger message scatter: {e}")
        raise ValueError(f"Cannot start pipeline - trigger failed: {e}")
    
    # task 2: monitoring queue and collect all messages
    print(f"\nTask 2: Monitoring queue and collecting messages...")
    print(f"Expected messages: {expected_count}")
    print(f"Messages have random delays from 30-900 seconds")
    print(f"This may take up to 15 minutes...\n")
    
    collected_messages = []
    start_time = time.time()
    last_message_time = start_time
    consecutive_empty_polls = 0
    message_timestamps = []
    
    # adaptive polling params
    min_poll_interval = 5   # min. wait between polls
    max_poll_interval = 30  # max. wait between polls
    current_poll_interval = poll_interval
    
    while len(collected_messages) < expected_count:
        elapsed = time.time() - start_time
        
        if elapsed > max_wait_time:
            print(f"\nTimeout reached after {int(elapsed)}s")
            print(f"Collected {len(collected_messages)}/{expected_count} messages")
            break
        
        # checking queue attributes before receiving messages to avoid unnecessary receive_message calls when queue is empty
        queue_stats = get_queue_attributes(sqs_client, sqs_url)
        
        total_in_queue = (queue_stats['available'] + 
                         queue_stats['not_visible'] + 
                         queue_stats['delayed'])
        
        print(f"[{int(elapsed)}s] Queue status:")
        print(f"  Available: {queue_stats['available']}")
        print(f"  Not Visible: {queue_stats['not_visible']}")
        print(f"  Delayed: {queue_stats['delayed']}")
        print(f"  Collected: {len(collected_messages)}/{expected_count}")

        # only fetch messages if there are available messages
        if queue_stats['available'] > 0:
            print(f"Messages available! Fetching now...")
            consecutive_empty_polls = 0  # resetting the counter
            
            messages = receive_messages_batch(sqs_client, sqs_url)
            
            if messages:
                current_batch_time = time.time()
                
                for msg in messages:
                    try:
                        # processing message
                        processed = process_message(msg)
                        collected_messages.append(processed)
                        message_timestamps.append(current_batch_time)
                        last_message_time = current_batch_time
                        
                        # deleteing the message -- don't let delete failures stop processing
                        delete_success = delete_message(
                            sqs_client,
                            sqs_url,
                            processed['receipt_handle'],
                            processed['message_id']
                        )
                        
                        if not delete_success:
                            print(f"Warning: Message {processed['message_id']} not deleted but will continue")

                    except Exception as e:
                        # if there just happens to be an unexpected error
                        print(f"Unexpected error handling message: {e}")
                        continue
    
    # if there were no messages collected at all
    if len(collected_messages) == 0:
        error_msg = "No messages collected from queue?!?"
        print(f"\nERROR: {error_msg}")
        raise ValueError(error_msg)
    
    print(f"\nSQS Collection Summary:")
    print(f" Messages collected: {len(collected_messages)}")
    print(f" Time elapsed: {int(time.time() - start_time)}s")
    
    # task 3: assembling phrase and submit answer
    print(f"\nTask 3: Assembling and submitting answer...")
    
    try:
        complete_phrase = assemble_phrase(collected_messages)
    except Exception as e:
        print(f"Failed to assemble phrase: {e}")
        raise ValueError(f"Cannot assemble phrase from messages: {e}")
    
    try:
        submission_result = submit_answer(
            sqs_client,
            submit_queue_url,
            uvaid,
            complete_phrase,
            platform="prefect"
        )
    except Exception as e:
        print(f"Failed to submit answer: {e}")
        raise ValueError(f"Cannot submit answer to queue: {e}")
    

    print("Flow success!")

    
    return {
        'status': 'success',
        'uvaid': uvaid,
        'messages_collected': len(collected_messages),
        'phrase': complete_phrase,
        'submission_message_id': submission_result['message_id'],
        'total_time': time.time() - start_time
    }


if __name__ == "__main__":
    # running the flow
    result = sqs_message_puzzle_flow()
    
    # final results 
    print("Final results:")
    print(f"Status: {result['status']}")
    print(f"Messages Processed: {result['messages_collected']}")
    print(f"Complete Phrase: '{result['phrase']}'")
    print(f"Submission Message ID: {result['submission_message_id']}")
    print(f"Total Time: {int(result['total_time'])}s")