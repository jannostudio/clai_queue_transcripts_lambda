import json
import logging
import os
import pickle
from datetime import datetime
from typing import Dict, Set, Tuple, Union

import boto3
import pandas as pd

from utils import cleaning

s3 = boto3.client('s3')
BUCKET_NAME = 'clai-video-ids'
CLAI_EXPORTS_BUCKET_NAME = "clai-youtube-exports"
CLAI_LIGHT_EXPORTS_BUCKET_NAME = "clai-light-youtube-exports"
FILE_NAME = 'all/all.pickle'

logger = logging.getLogger('cloudwatch_logger')


def download_and_preprocess_data(event: Dict) -> Tuple[pd.DataFrame, Dict[str, Union[str, str]]]:
    """Download and preprocess data from S3.

    Args:
        event (Dict): The Lambda event object.

    Returns:
        Tuple: The preprocessed data as a Pandas DataFrame and metadata as a dictionary.
    """
    try:
        records = json.loads(event['Records'][0]['body'])['Records']
        s3_bucket = records[0]['s3']['bucket']['name']
        s3_key = records[0]['s3']['object']['key']

        # Remove the '.json' extension from the s3_key
        s3_key_base, _ = os.path.splitext(s3_key)

        # Fetching the upload timestamp from the event
        upload_timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        raw_data = s3.get_object(Bucket=s3_bucket, Key=s3_key)['Body'].read().decode('utf-8')

        if s3_bucket == CLAI_EXPORTS_BUCKET_NAME:
            # Cleaning step for the first bucket
            df = cleaning.preprocess_data_clai(raw_data)
        elif s3_bucket == CLAI_LIGHT_EXPORTS_BUCKET_NAME:
            # Cleaning step for the second bucket
            df = cleaning.preprocess_data_clai_light(raw_data)
        else:
            logger.error(f"Unknown bucket: {s3_bucket}")
            raise ValueError(f"Unknown bucket: {s3_bucket}")

        metadata = {
            'file_id': s3_key_base,
            's3_bucket': s3_bucket,
            'upload_timestamp': upload_timestamp
        }

        return df, metadata
    except Exception as e:
        logger.error(f"Failed to download and preprocess data: {e}")
        raise


def download_existing_video_ids() -> Set[str]:
    """Download the existing set of video IDs from S3.

    Returns:
        Set[str]: The existing set of video IDs.
    """
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_NAME)
        body = response['Body'].read()
        return set(pickle.loads(body))
    except Exception as e:
        # Replace with your logging or error handling mechanism
        logger.error(f"Failed to download existing video IDs: {e}")
        raise
