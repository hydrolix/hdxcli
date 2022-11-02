from datetime import datetime


def get_datetime_from_formatted_string(date_str):
    """Get a datetime object from a formatted string as done in Json
    config files for hdx-cli"""
    return datetime.strptime(date_str,
                             '%Y-%m-%d %H:%M:%S.%f')
