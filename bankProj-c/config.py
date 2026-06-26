# config.py
class SystemConfig:
    def __init__(
        self,
        process_count=2,
        thread_count=12,
        aging_threshold=15,
        enable_banker=False,
        enable_mlfq=False
    ):
        self.process_count = process_count
        self.thread_count = thread_count
        self.aging_threshold = aging_threshold
        self.enable_banker = enable_banker
        self.enable_mlfq = enable_mlfq
