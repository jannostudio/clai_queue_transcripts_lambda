import json
from typing import List, Dict, Set

import boto3
import pandas as pd

sqs_client = boto3.client('sqs')
QUEUE_URL = 'https://sqs.eu-central-1.amazonaws.com/800678845068/clai_get_transcript_queue'


def prepare_sqs_messages(df: pd.DataFrame, existing_ids: Set[str], file_id: str) -> List[Dict]:
    """Prepare SQS messages based on the preprocessed DataFrame and existing IDs.

    Args:
        df (pd.DataFrame): The preprocessed DataFrame.
        existing_ids (Set[str]): The existing video IDs.

    Returns:
        List[Dict]: A list of SQS message dictionaries.
    """
    # Step 1: Filter out the DataFrame rows with video_ids in existing_ids
    filtered_df = df[~df['video_id'].isin(existing_ids)].copy(deep=True)

    # Step 2: Reset the index of the filtered DataFrame
    filtered_df.reset_index(drop=True, inplace=True)

    # Step 3: Calculate the total number of rows in the DataFrame
    total_rows = len(filtered_df)

    messages = []

    # Step 4: Iterate through the filtered DataFrame
    for idx, record in filtered_df.iterrows():
        current_transcript = idx + 1
        video_id = record['video_id']

        # Add the new video_id to existing_ids
        existing_ids.add(video_id)

        # Prepare the SQS message dictionary
        message_dict = {
            'Id': str(video_id),
            'MessageBody': json.dumps({
                'file_id': {'S': str(file_id)},
                'video_id': {'S': str(video_id)},
                'channel_name': {'S': str(record.get('channel_name', ''))},
                'status': {'S': 'NEW'},
                'type': {'S': str(record.get('type', ''))},
                'title': {'S': str(record.get('title', ''))},
                'watched_at': {'S': str(record.get('watched_at', ''))},
                'current_transcript': {'N': current_transcript},
                'total_transcripts': {'N': total_rows}
            })
        }

        # Add the message dictionary to the list
        messages.append(message_dict)

    return messages


def send_messages_to_sqs(messages: List[Dict]) -> int:
    """Send messages to SQS in batches.

    Args:
        messages (List[Dict]): A list of SQS message dictionaries.

    Returns:
        int: The number of successfully sent messages.
    """
    batch_size = 10
    successful_messages = 0
    for i in range(0, len(messages), batch_size):
        batch = messages[i:i + batch_size]
        response = sqs_client.send_message_batch(QueueUrl=QUEUE_URL, Entries=batch)
        successful_messages += len(response['Successful'])
    return successful_messages
