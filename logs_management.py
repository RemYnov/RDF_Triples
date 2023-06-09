import time
from colorama import Fore, Style


class Logger:
    """
    This class allow to track logs activity and manage the way
    we display the information to the console
    """
    def __init__(self, prefix="-", defaultCustomLogs="normal"):
        self.logs = []
        self.counters = {}
        self.timestamps = {}
        self.prefix = prefix

        self.customLog = defaultCustomLogs
        self.HEADER = Fore.MAGENTA
        self.BLUE = Fore.BLUE
        self.GREEN = Fore.GREEN
        self.YELLOW = Fore.YELLOW
        self.RED = Fore.RED
        self.RESET = Style.RESET_ALL

    def log(self, message, display=True, level="normal", counter=None):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        log_entry = f"{timestamp} {self.prefix} {message}"
        self.logs.append(log_entry)
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
