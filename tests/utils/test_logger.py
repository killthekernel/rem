import logging
import tempfile
from pathlib import Path

from rem.utils.logger import get_logger


def test_logger_and_log_file() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        log_file = Path(tmpdir) / "test_log.txt"
        logger = get_logger(level="DEBUG", log_file=log_file)

        logger.info("Hello test!")
        logger.debug("Debug message")

        # Flush and remove the FileHandler to release the file
        for handler in logger.handlers:
            if isinstance(handler, logging.FileHandler):
                handler.flush()
                handler.close()
        logger.handlers = [
            h for h in logger.handlers if not isinstance(h, logging.FileHandler)
        ]

        with open(log_file, encoding="utf-8") as f:
            contents = f.read()
            assert "Hello test!" in contents
            assert "Debug message" in contents
