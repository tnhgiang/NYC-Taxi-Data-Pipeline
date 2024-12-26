from datetime import datetime

import pytz


def get_current_time():
    """Get current time in Vietnam timezone."""
    vietname_tz = pytz.timezone("Asia/Ho_Chi_Minh")
    return datetime.now(vietname_tz).strftime("%Y-%m-%d_%H:%M:%S:%f")
