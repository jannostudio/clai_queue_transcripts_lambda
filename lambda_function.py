"""AWS Lambda function for processing YouTube watch history data.

This script performs several operations, such as data preprocessing, message
queueing, and duplicate detection, to manage YouTube watch history data.
"""

import boto3
import logging
import pickle
from datetime import datetime
from typing import Dict
from utils.data_processing import download_and_preprocess_data, download_existing_video_ids, BUCKET_NAME, FILE_NAME
from utils.queue_management import prepare_sqs_messages, send_messages_to_sqs

# Initialize AWS services
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Initialize the DynamoDB table
table = dynamodb.Table('clai_files_status')

# This is the labeling queue
sqs = boto3.client('sqs')
labeling_queue = 'https://sqs.eu-central-1.amazonaws.com/800678845068/clai_process_file_queue.fifo'


# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict, context: Dict) -> Dict:
    try:
        df, metadata = download_and_preprocess_data(event)
        existing_ids = download_existing_video_ids()
        # df = df.sample(3)
        # existing_ids = set(["uoeWaYPkAIc"])
        num_ids_before = len(existing_ids)

        messages = prepare_sqs_messages(df, existing_ids, str(metadata["file_id"]))
        successful_messages = send_messages_to_sqs(messages)

        num_ids_after = len(existing_ids)

        result = {
            'num_videos_before': num_ids_before,
            'num_videos_after': num_ids_after,
            'new_videos_added': num_ids_after - num_ids_before,
            'duplicates_not_added': df.shape[0] - (num_ids_after - num_ids_before),
            'messages_sent': successful_messages
        }
        num_videos_unique = result['duplicates_not_added'] + result['new_videos_added']
        result.update(metadata)

        # Prepare the item dictionary, conditionally adding 'transcripts_last' if no messages
        item = {
            'file_id': result['file_id'],
            'file_origin_s3_bucket': result['s3_bucket'],
            'transcripts_first': result['upload_timestamp'],
            'num_videos_file_unique': str(num_videos_unique),
            'num_videos_total_before': str(result['num_videos_before']),
            'num_videos_total_after': str(result['num_videos_after']),
            'num_videos_total_added': str(result['new_videos_added']),
            'num_videos_total_duplicate': str(result['duplicates_not_added']),
            'messages_sent': str(result['messages_sent'])
        }

        if len(messages) == 0:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            item['transcripts_last'] = f"{current_time}"

        # Writing to DynamoDB
        table.put_item(Item=item)

        # Update S3 with the new set of video IDs
        if result['new_videos_added'] > 0:
            s3.put_object(Body=pickle.dumps(existing_ids), Bucket=BUCKET_NAME, Key=FILE_NAME)

        try:
            incoming_body = event.get('Records', [{}])[0].get('body', '{}')
            sqs_response = sqs.send_message(
                QueueUrl=labeling_queue,
                MessageBody=incoming_body,
                MessageGroupId=result['file_id'],
                MessageDeduplicationId=f"{result['file_id']}_{datetime.now().timestamp()}"
            )
            logger.info(f"Message send to labeling queue: {sqs_response}")
        except Exception as sqs_exception:
            logger.error(f"Failed to send message to labeling queue: {sqs_exception}")

        logger.info('Execution successful. Result: %s', result)
        return result
    except Exception as e:
        logger.error('Execution failed: %s', e)
        raise e
