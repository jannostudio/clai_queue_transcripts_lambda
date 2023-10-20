import json
import logging
from typing import List, Dict, Any
import pandas as pd
from dateutil import parser
from pandas import json_normalize

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_video_id(url: str) -> str:
    """Extracts the ID of a YouTube video from its URL.

    Args:
        url: The URL of a YouTube video.

    Returns:
        The ID of the video.

    Examples:
        >>> get_video_id('https://www.youtube.com/watch?v=dQw4w9WgXcQ')
        'dQw4w9WgXcQ'
    """
    return url.split('=')[-1]


def extract_channel_name(subtitles: List[Dict[str, Any]]) -> str:
    """Extracts the channel name from the subtitles information.

    Args:
        subtitles: The subtitles data, which is a list of dictionaries.

    Returns:
        The channel name.
    """
    return list(subtitles[0].values())[0]


def preprocess_data_clai(raw_data: str) -> pd.DataFrame:
    """Preprocesses a raw JSON string of YouTube watch history data.

    The function performs the following operations:
    1. Parses the raw JSON data into a DataFrame.
    2. Drops rows with missing values in specific columns.
    3. Extracts the channel name and video ID.
    4. Removes duplicate video IDs, keeping only the first occurrence.
    5. Adds a 'status' column with the value 'CREATED'.
    6. Renames the 'time' column to 'watched_at'.

    Args:
        raw_data (str): The input raw JSON string.

    Returns:
        pd.DataFrame: The preprocessed DataFrame.

    Example:
        >>> raw_data = 'your_raw_json_string_here'
        >>> preprocessed_df = preprocess_data_clai(raw_data)
    """

    # Parse raw JSON data into a DataFrame
    df = pd.DataFrame(json.loads(raw_data))

    # Drop rows with missing values in specific columns and create a copy
    df = df.dropna(subset=["titleUrl", "title", "subtitles", "time"]).copy()

    # Extract channel name and video ID
    df['channel_name'] = df['subtitles'].apply(extract_channel_name)
    df['video_id'] = df['titleUrl'].apply(get_video_id)

    # Filter out invalid video IDs and create a copy
    df = df[df['video_id'].str.len() == 11].copy()

    # Drop duplicates based on video_id and reset index
    df.drop_duplicates("video_id", inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Add 'status' column and rename 'time' to 'watched_at'
    df['status'] = 'CREATED'
    df.rename(columns={"time": "watched_at"}, inplace=True)

    return df


def rename_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Renames columns of a DataFrame to conform to a standardized naming convention.

    Args:
        df (DataFrame): The input DataFrame with original column names.

    Returns:
        DataFrame: A new DataFrame with renamed columns.
    """
    renamed_df = df.rename(columns={
        "channelName": "channel_name",
        "timestamp": "watched_at",
    })
    return renamed_df


def convert_to_datetime(df, column_name="watched_at"):
    # Function to handle datetime conversion
    def to_datetime(val):
        try:
            # Parse the date
            dt = parser.isoparse(val)
            
            # Truncate to your required precision (up to seconds and two decimal places)
            dt = dt.replace(microsecond=int(dt.microsecond / 10000) * 10000)
            return dt
        except ValueError:
            logger.info(f"ValueError: Could not convert {val} to datetime.")
            return pd.NaT  # Return Not a Time for unparseable formats
    
    # Apply the conversion function to the DataFrame column
    df[column_name] = df[column_name].apply(to_datetime)
    
    if df[column_name].isna().any():
        logger.warning(f"Some values in {column_name} could not be converted to datetime.")
    else:
        logger.info(f"Successfully converted {column_name} to datetime.")


def calculate_sub_group_rank(df, groupby_column='channel_name', datetime_column='watched_at'):
    """Calculates the sub-group ranking based on the latest datetime."""
    sub_group_rank = df.groupby(groupby_column)[datetime_column].max().reset_index()
    sub_group_rank['sub_group_rank'] = sub_group_rank[datetime_column].rank(
        method='first', ascending=False)
    return sub_group_rank.drop(datetime_column, axis=1)


def rank_within_group(df, groupby_column='channel_name', datetime_column='watched_at'):
    """Ranks each entry within its subgroup based on datetime."""
    df['video_rank_within_group'] = df.groupby(
        groupby_column)[datetime_column].rank(method='first', ascending=False)


def sort_dataframe(df, sort_columns=['video_rank_within_group', 'sub_group_rank', 'type'],
                   ascending_flags=[True, True, True]):
    """Sorts the DataFrame based on the given columns and flags."""
    return df.sort_values(by=sort_columns, ascending=ascending_flags)


def move_type_to_top(df, type_value='like', column_name='type'):
    """Moves rows with a specific 'type' to the top of the DataFrame."""
    df_like = df[df[column_name] == type_value]
    df_other = df[df[column_name] != type_value]
    return pd.concat([df_like, df_other])


def reorder_frame(df):
    """Orders the rows in the DataFrame by priority, meaning higher scoring rows are more important."""
    # Convert 'watched_at' to datetime for proper sorting
    convert_to_datetime(df)

    # Step 1: Create a ranking for sub-groups based on the latest 'watched_at'
    sub_group_rank = calculate_sub_group_rank(df)

    # Merge the sub_group_rank back to the original DataFrame
    df = df.merge(sub_group_rank, on='channel_name', how='left')

    # Step 2: Create a second rank that ranks each video within its sub-group
    rank_within_group(df)

    # Step 3: Sort the DataFrame based on the ranks created
    df = sort_dataframe(df)

    # Step 4: Move rows with 'like' in the 'type' column to the top
    df = move_type_to_top(df)

    # Step 5: Reset the index
    df = df.reset_index(drop=True)

    return df.drop_duplicates("video_id").drop(columns=["sub_group_rank", "video_rank_within_group"])


def preprocess_data_clai_light(raw_data: str) -> pd.DataFrame:
    # Parse the raw JSON string into a Python dictionary
    data = json.loads(raw_data)

    # Extract 'items' from the root
    items_data = data.get('items', {})

    # Extract 'likes' and 'subs' from 'items'
    likes_data = items_data.get('likes', [])
    subs_data = items_data.get('subs', [])

    # Normalize these lists into separate DataFrames
    df_likes = pd.json_normalize(likes_data)
    df_subs = json_normalize(subs_data)

    # Add 'order' columns to preserve internal order of each DataFrame
    df_likes['order'] = range(len(df_likes))
    df_subs['order'] = range(len(df_subs))

    # Add a 'type' column to distinguish between 'likes' and 'subs'
    df_likes['type'] = 'like'
    df_subs['type'] = 'sub'

    # Concatenate the two DataFrames
    df_concatenated = pd.concat([df_likes, df_subs], ignore_index=True)

    # Sort by the 'type' and 'order' columns to maintain original internal order
    df_concatenated.sort_values(by=['type', 'order'], inplace=True)

    # Remove the 'order' column, if you don't need it anymore
    df_concatenated.drop(columns=['order'], inplace=True)

    df_concatenated = df_concatenated.dropna(subset = ["url", "title", "channelName"])
    # [["url", "timestamp", "title", "type"]]
    df_concatenated["video_id"] = df_concatenated.url.apply(lambda x: get_video_id(x))

    df_concatenated = df_concatenated[["video_id", "timestamp", "title", "channelName", "type"]]

    df_concatenated = rename_dataframe(df_concatenated)

    df_concatenated = reorder_frame(df_concatenated)
    return df_concatenated