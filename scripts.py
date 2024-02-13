import os
import subprocess
import time
from pathlib import Path

from watchdog.events import DirModifiedEvent, FileModifiedEvent, FileSystemEventHandler
from watchdog.observers import Observer

WATCH_DIRS = [Path("src"), Path("tests")]
WATCH_SUFFIXES = [".py"]


class Watcher(FileSystemEventHandler):
    """
    Monitors file system events and triggers actions accordingly.
    """

    def on_modified(self, event: DirModifiedEvent | FileModifiedEvent):
        match event:
            case FileModifiedEvent(src_path=filepath):
                filepath = Path(filepath)
                if not any(filepath.is_relative_to(wdir) for wdir in WATCH_DIRS):
                    return
                if filepath.suffix not in WATCH_SUFFIXES:
                    return
            case _:
                return

        run_tests()


def run_tests():
    """
    Runs the unit tests using the `unittest` module.
    """
    os.system("clear && printf '\\e[3J'")
    subprocess.call("python -m unittest discover -s tests".split())


def run_watch():
    """
    Run the unit tests when a file changes.
    """

    event_handler = Watcher()
    observer = Observer()
    observer.schedule(event_handler, path=".", recursive=True)
    observer.start()

    try:
        run_tests()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
