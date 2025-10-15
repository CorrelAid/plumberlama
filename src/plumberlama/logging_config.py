import logging

from rich.logging import RichHandler


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Configure logging with Rich handler."""
    # Create logger
    logger = logging.getLogger("plumberlama")
    logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()

    # Add Rich handler
    rich_handler = RichHandler(
        rich_tracebacks=True,
        markup=True,
        show_time=True,
        show_path=False,
    )
    rich_handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(rich_handler)

    return logger


def get_logger(name: str = "plumberlama") -> logging.Logger:
    """Get logger instance."""
    return logging.getLogger(name)
