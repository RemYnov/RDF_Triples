import time
from config import TELEGRAM_BOT_TOKEN, BOT_CHAT_ID
from colorama import Fore, Style
import os
from telegram import Update, Bot
import asyncio
import re
import html


class Logger:
    """
    This class allow to track logs activity and manage the way
    we display the information to the console
    """
    def __init__(self, prefix="-", defaultCustomLogs="normal", botEnabled=False, runName=""):
        self.logs = []
        self.counters = {}
        self.timestamps = {}
        self.prefix = prefix

        if botEnabled:
            self.bot_logger = TelegramLogger(self, run_name=runName, telegram_bot_token=TELEGRAM_BOT_TOKEN, chat_id=BOT_CHAT_ID)
        self.bot_enable = botEnabled

        self.customLog = defaultCustomLogs
        self.HEADER = Fore.MAGENTA
        self.BLUE = Fore.BLUE
        self.GREEN = Fore.GREEN
        self.YELLOW = Fore.YELLOW
        self.RED = Fore.RED
        self.RESET = Style.RESET_ALL

    def log(self, message, display=True, level="normal", counter=None, isTitle=False):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        log_entry = f"{timestamp} {self.prefix} {message}"
        self.logs.append(log_entry)

        if self.bot_enable:
            # Send to Telegram Chat bot
            self.bot_logger.send_message_to_telegram(message, isTitle)
        if display:
            self.colored_display(log_entry, level=self.customLog)
        if counter:
            self.custom_counter(counter)

    def colored_display(self, msg, level):
        if level == "normal":
            print(msg)
        elif level == "fancy":
            print(self.BLUE + msg + self.RESET)
        elif level == "warning":
            print(self.YELLOW + msg + self.RESET)
        elif level == "critical":
            print(self.RED + msg + self.RESET)

    def custom_counter(self, counter, increment=1):
        if counter not in self.counters:
            self.counters[counter] = 0
        self.counters[counter] += increment

    def get_counter(self, counter):
        return self.counters[counter]

    def start_timer(self, timer_name):
        self.timestamps[timer_name] = time.time()

    def stop_timer(self, timer_name):
        elapsed_time = time.time() - self.timestamps[timer_name]
        self.log(f"{timer_name} done in : {elapsed_time:.2f} seconds")

    def get_logs(self):
        return "\n".join(self.logs)

    def get_log_dict(self):
        return {
            "logs": self.logs,
            "counters": self.counters,
            "timestamps": self.timestamps
        }

    def get_timer_counter(self):
        return {
            "counters": self.counters,
            "timestamps": self.timestamps
        }


class TelegramLogger:
    def __init__(self, logger, run_name, telegram_bot_token, chat_id):
        self.logger = logger
        self.bot = Bot(token=telegram_bot_token)
        self.chat_id = chat_id

        if run_name != "":
            self.send_message_to_telegram("N E W  R U N : " + run_name, title=True)
            #self.send_message_to_telegram(run_name, title=True)

    def markdown_v2(self, msg):
        escape_chars = r'\*_`\[\]()~>#\+\-=\|{}.!'
        escaped_msg = re.sub(r'([{}])'.format(re.escape(escape_chars)), r'\\\1', msg)
        return escaped_msg

    def send_message_to_telegram(self, message, title=False):
        loop = asyncio.get_event_loop()  # To avoid error when sending multiple logs
        formated_msg = self.markdown_v2(message)
        if title:
            formated_msg = f"*{formated_msg}*"

        if loop.is_running():
            asyncio.create_task(self.async_send_message_to_telegram(formated_msg))
        else:
            loop.run_until_complete(self.async_send_message_to_telegram(formated_msg))

    async def async_send_message_to_telegram(self, message):
        await self.bot.send_message(chat_id=self.chat_id, text=message, parse_mode="MarkdownV2")
