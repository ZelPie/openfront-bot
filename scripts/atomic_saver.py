import os
import ujson as json
import time
import asyncio
from datetime import datetime
import os
import time
import asyncio
import uuid

def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

class AtomicSaver:
    @staticmethod
    def save_json(file_path: str, data: dict | list | set, retries: int = 10, delay: float = 0.5):
        """
        Synchronously saves data to a JSON file atomically using a unique temporary file.
        Includes a retry loop to handle Windows file locking issues.
        """
        # Create a completely unique temporary filename to prevent race conditions
        unique_id = uuid.uuid4().hex
        temp_path = f"{file_path}.{unique_id}.tmp"
        
        # Ensure the target directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        try:
            # 1. Write to the safe, unique temporary file first
            with open(temp_path, "w") as f:
                json.dump(data, f, default=set_default)
                
            # 2. Instantly swap it using a bulletproof retry loop
            for i in range(retries):
                try:
                    os.replace(temp_path, file_path)
                    break  # Success! Break out of the retry loop
                except (PermissionError, OSError) as e:
                    if i == retries - 1:
                        print(f"Failed to atomically save {file_path}: {e}")
                        raise e  # If it failed after all retries, throw the error
                    time.sleep(delay)
                    
        finally:
            # 3. Cleanup: If something catastrophic happens and the swap fails, 
            # ensure we delete the orphaned unique .tmp file so they don't pile up.
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass

    @staticmethod
    async def save_json_async(file_path: str, data: dict | list | set, lock: asyncio.Lock = None, retries: int = 10, delay: float = 0.5):
        """
        Asynchronously saves data to a JSON file atomically.
        Takes an optional asyncio.Lock() to prevent race conditions.
        """
        if lock:
            async with lock:
                await asyncio.to_thread(AtomicSaver.save_json, file_path, data, retries, delay)
        else:
            await asyncio.to_thread(AtomicSaver.save_json, file_path, data, retries, delay)

class PerfTimer:
    """A simple context manager for profiling code execution time."""

    PRINT_TO_CONSOLE = False
    LOG_TO_FILE = True
    LOG_FILE_PATH = "./bot_data/perf_log.txt"

    def __init__(self, name="Task"):
        self.name = name

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # If both settings are off, skip everything to save CPU
        if not PerfTimer.PRINT_TO_CONSOLE and not PerfTimer.LOG_TO_FILE:
            return

        elapsed = time.perf_counter() - self.start
        
        # Format the message
        if elapsed > 1.0:
            msg = f"[{self.name}] took {elapsed:.2f}s (SLOW)"
        else:
            msg = f"[{self.name}] took {elapsed * 1000:.2f}ms"

        # 1. Terminal Output
        if PerfTimer.PRINT_TO_CONSOLE:
            print(f"[TIMER] {msg}")
            
        # 2. File Output
        if PerfTimer.LOG_TO_FILE:
            # Create the folder if it doesn't exist yet
            os.makedirs(os.path.dirname(PerfTimer.LOG_FILE_PATH), exist_ok=True)
            
            # Add a timestamp so you know when the lag spike happened
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_entry = f"[{timestamp}] {msg}\n"
            
            with open(PerfTimer.LOG_FILE_PATH, "a", encoding="utf-8") as f:
                f.write(log_entry)