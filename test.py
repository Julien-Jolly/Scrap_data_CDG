from src.downloader import download_files
import queue
status_queue = queue.Queue()
download_files(["Source10"], status_queue)