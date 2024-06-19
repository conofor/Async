import sys
import asyncio
import aiofiles
from datetime import datetime

from .LockManager import lock_manager

class DummyStream:
    def write(self, *args, **kwargs):
        pass

    def flush(self, *args, **kwargs):
        pass

class LoggerAsync:
    _instance = None

    def __new__(cls, filename="logfile.txt"):
        with lock_manager.this():
            if cls._instance is None:
                cls._instance = super(LoggerAsync, cls).__new__(cls)
                cls._instance.init(filename)
        return cls._instance

    def init(self, filename):
        # Use DummyStream if stdout or stderr are None
        self.stdout = sys.stdout if sys.stdout else DummyStream()
        self.stderr = sys.stderr if sys.stderr else DummyStream()
        sys.stdout = self
        sys.stderr = self

        self.filename = filename

        # Add the date and underscores to the log file
        date_line = f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]\n"
        asyncio.run(self._write_async(date_line))

    def write(self, message):
        # Write to the terminal
        if self.stdout:
            self.stdout.write(message)

        # Schedule the async write operation in the running event loop if available, else run it
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._write_async(message))
        except RuntimeError:
            asyncio.run(self._write_async(message))

    async def _write_async(self, message):
        async with lock_manager.this():
            async with aiofiles.open(self.filename, 'a', encoding='utf-8') as logfile:
                await logfile.write(message)

    def flush(self):
        if self.stdout:
            self.stdout.flush()

    def __getattr__(self, attr):
        if self.stdout:
            return getattr(self.stdout, attr)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{attr}'")

# Ensure the logger is initialized as early as possible
logger = LoggerAsync()