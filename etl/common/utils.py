from datetime import datetime
from zoneinfo import ZoneInfo

class Now:
    """
    A utility class for handling current time in a specific timezone and logging messages
    with timestamps and customizable formatting.

    This class provides methods to retrieve the current time as a string or datetime object
    and to log messages with optional start/end separators and padding.

    Attributes:
        show (bool): Default flag to control whether log messages are displayed.
    """

    def __init__(self, show: bool = True):
        """
        Initializes the Now instance.

        Args:
            show (bool, optional): Default visibility for log messages. Defaults to True.
        """
        self.show = show

    @staticmethod
    def now(timezone: str = 'America/Sao_Paulo') -> str:
        """
        Returns the current time as a formatted string in the specified timezone.

        Args:
            timezone (str, optional): Timezone name (e.g., 'America/Sao_Paulo').
                Defaults to 'America/Sao_Paulo'.

        Returns:
            str: Formatted current time (YYYY-MM-DD HH:MM:SS).

        Raises:
            zoneinfo.ZoneInfoNotFoundError: If the timezone is invalid.
        """
        return datetime.now(ZoneInfo(timezone)).strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def now_datetime(timezone: str = 'America/Sao_Paulo') -> datetime:
        """
        Returns the current time as a datetime object in the specified timezone.

        Args:
            timezone (str, optional): Timezone name (e.g., 'America/Sao_Paulo').
                Defaults to 'America/Sao_Paulo'.

        Returns:
            datetime: Current datetime object in the specified timezone.

        Raises:
            zoneinfo.ZoneInfoNotFoundError: If the timezone is invalid.
        """
        return datetime.now(ZoneInfo(timezone))

    def log_message(self,
                    message: str,
                    start: bool = False,
                    end: bool = False,
                    sep: str = '-',
                    line_length: int = 120,
                    show: bool | None = None) -> None:
        """
        Logs a message with a timestamp, optional start/end lines, and padding.

        The message is printed in the format: "TIMESTAMP | MESSAGE | [padding]".
        Padding fills the line to `line_length` using `sep`. If the content exceeds
        `line_length`, no padding is added.

        Args:
            message (str): The message to log.
            start (bool, optional): If True, prints a full separator line before the message.
                Defaults to False.
            end (bool, optional): If True, prints a full separator line after the message.
                Defaults to False.
            sep (str, optional): Separator character(s) for padding and lines. Defaults to '-'.
            line_length (int, optional): Target length for the log line. Defaults to 120.
            show (bool | None, optional): If True, display the log; if False, suppress.
                If None, uses self.show. Defaults to None.

        Returns:
            None
        """
        if show is None:
            show = self.show

        if not show:
            return

        timestamp = self.now()
        sep_len = len(sep)
        extra_len = len(timestamp) + len(message) + 5  # For " | " and " |"

        if start:
            print(sep * line_length)

        if extra_len >= line_length:
            # No padding if content is too long
            print(f"{timestamp} | {message} |")
            if end:
                print(sep * line_length)
            return

        filler = ""
        if sep_len > 0:
            remaining = line_length - extra_len
            length_fill_line = remaining // sep_len
            total_filled = length_fill_line * sep_len
            complement = sep[:remaining - total_filled] if remaining > total_filled else ""
            filler = (sep * length_fill_line) + complement

        print(f"{timestamp} | {message} |{filler}")

        if end:
            print(sep * line_length)

def delta_logos(delta_layer:str) -> None:
    delta_layer: str = delta_layer.lower().strip()

    if delta_layer not in ['bronze', 'silver', 'gold']:
        raise TypeError('type must be either "bronze", "silver" or "gold"')

    if delta_layer == 'bronze':
        print(r"____/\\\\\\\\\\\\\______/\\\\\\\\\___________/\\\\\_______/\\\\\_____/\\\__/\\\\\\\\\\\\\\\__/\\\\\\\\\\\\\\\___________")
        print(r"____\/\\\/////////\\\__/\\\///////\\\_______/\\\///\\\____\/\\\\\\___\/\\\_\////////////\\\__\/\\\///////////___________")
        print(r"_____\/\\\_______\/\\\_\/\\\_____\/\\\_____/\\\/__\///\\\__\/\\\/\\\__\/\\\___________/\\\/___\/\\\_____________________")
        print(r"______\/\\\\\\\\\\\\\\__\/\\\\\\\\\\\/_____/\\\______\//\\\_\/\\\//\\\_\/\\\_________/\\\/_____\/\\\\\\\\\\\____________")
        print(r"_______\/\\\/////////\\\_\/\\\//////\\\____\/\\\_______\/\\\_\/\\\\//\\\\/\\\_______/\\\/_______\/\\\///////____________")
        print(r"________\/\\\_______\/\\\_\/\\\____\//\\\___\//\\\______/\\\__\/\\\_\//\\\/\\\_____/\\\/_________\/\\\__________________")
        print(r"_________\/\\\_______\/\\\_\/\\\_____\//\\\___\///\\\__/\\\____\/\\\__\//\\\\\\___/\\\/___________\/\\\_________________")
        print(r"__________\/\\\\\\\\\\\\\/__\/\\\______\//\\\____\///\\\\\/_____\/\\\___\//\\\\\__/\\\\\\\\\\\\\\\_\/\\\\\\\\\\\\\\\____")
        print(r"___________\/////////////____\///________\///_______\/////_______\///_____\/////__\///////////////__\///////////////____")
    if delta_layer == 'silver':
        print(r"________/\\\\\\\\\\\____/\\\\\\\\\\\__/\\\______________/\\\________/\\\__/\\\\\\\\\\\\\\\____/\\\\\\\\\________________")
        print(r"_______/\\\/////////\\\_\/////\\\///__\/\\\_____________\/\\\_______\/\\\_\/\\\///////////___/\\\///////\\\_____________")
        print(r"_______\//\\\______\///______\/\\\_____\/\\\_____________\//\\\______/\\\__\/\\\_____________\/\\\_____\/\\\____________")
        print(r"_________\////\\\_____________\/\\\_____\/\\\______________\//\\\____/\\\___\/\\\\\\\\\\\_____\/\\\\\\\\\\\/____________")
        print(r"_____________\////\\\__________\/\\\_____\/\\\_______________\//\\\__/\\\____\/\\\///////______\/\\\//////\\\___________")
        print(r"_________________\////\\\_______\/\\\_____\/\\\________________\//\\\/\\\_____\/\\\_____________\/\\\____\//\\\_________")
        print(r"___________/\\\______\//\\\______\/\\\_____\/\\\_________________\//\\\\\______\/\\\_____________\/\\\_____\//\\\_______")
        print(r"___________\///\\\\\\\\\\\/____/\\\\\\\\\\\_\/\\\\\\\\\\\\\\\______\//\\\_______\/\\\\\\\\\\\\\\\_\/\\\______\//\\\_____")
        print(r"______________\///////////_____\///////////__\///////////////________\///________\///////////////__\///________\///_____")
    if delta_layer == 'gold':
        print('_' * 19 + r"_____/\\\\\\\\\\\\_______/\\\\\_______/\\\______________/\\\\\\\\\\\\____________" + '_' * 20)
        print('_' * 19 + r"____/\\\//////////______/\\\///\\\____\/\\\_____________\/\\\////////\\\_________" + '_' * 20)
        print('_' * 19 + r"____/\\\_______________/\\\/__\///\\\__\/\\\_____________\/\\\______\//\\\_______" + '_' * 20)
        print('_' * 19 + r"____\/\\\____/\\\\\\\__/\\\______\//\\\_\/\\\_____________\/\\\_______\/\\\______" + '_' * 20)
        print('_' * 19 + r"_____\/\\\___\/////\\\_\/\\\_______\/\\\_\/\\\_____________\/\\\_______\/\\\_____" + '_' * 20)
        print('_' * 19 + r"______\/\\\_______\/\\\_\//\\\______/\\\__\/\\\_____________\/\\\_______\/\\\____" + '_' * 20)
        print('_' * 19 + r"_______\/\\\_______\/\\\__\///\\\__/\\\____\/\\\_____________\/\\\_______/\\\____" + '_' * 20)
        print('_' * 19 + r"________\//\\\\\\\\\\\\/_____\///\\\\\/_____\/\\\\\\\\\\\\\\\_\/\\\\\\\\\\\\/____" + '_' * 20)
        print('_' * 19 + r"__________\////////////_________\/////_______\///////////////__\////////////_____" + '_' * 20)