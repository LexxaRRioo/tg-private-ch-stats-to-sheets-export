SHEET_CONFIGS = {
    "channels_daily": {
        "key_columns": ["channel_id"],
        "merge_columns": [
            "channel_name",
            "member_count",
            "messages_count",
            "last_message_id",
        ],
        "timestamp_column": "processed_at",
    },
    "channel_messages": {
        "key_columns": ["channel_id", "message_id", "word"],
        "merge_columns": ["date"],
        "timestamp_column": "processed_at",
    },
    "chat_topics_hourly": {
        "key_columns": ["chat_id", "topic_id", "hour"],
        "merge_columns": [
            "chat_name",
            "topic_name",
            "message_count",
            "first_message_id",
            "last_message_id",
        ],
        "timestamp_column": "processed_at",
    },
}
