import time

class Logger:
    def __init__(self):
        self.logs = []
        self.counters = {}
        self.timestamps = {}

    def log(self, message, display=True, counter=None):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        log_entry = f"{timestamp} - {message}"
        self.logs.append(log_entry)
        if display:
            print(log_entry)
        if counter:
            self.increment_counter(counter)
    def increment_counter(self, counter, increment=1):
        if counter not in self.counters:
            self.counters[counter] = 0
        self.counters[counter] += increment
    def start_timer(self, timer_name):
        self.timestamps[timer_name] = time.time()
    def stop_timer(self, timer_name):
        elapsed_time = time.time() - self.timestamps[timer_name]
        self.log(f"{timer_name} step duration: {elapsed_time:.2f} seconds")
    def get_logs(self):
        return "\n".join(self.logs)
    def get_log_dict(self):
        return {
            "logs": self.logs,
            "counters": self.counters,
            "timestamps": self.timestamps
        }