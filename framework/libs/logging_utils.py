import logging
import sys
from typing import Optional
from datetime import datetime


def get_logger(
    name: str,
    level: str = "INFO",
    log_to_file: bool = False,
    log_file_path: Optional[str] = None
) -> logging.Logger:
    """
    Get a logger with unified formatting.
    
    Args:
        name: Logger name
        level: Log level (DEBUG/INFO/WARNING/ERROR)
        log_to_file: Whether to write to file
        log_file_path: Log file path
    
    Returns:
        Logger object
    """
    logger = logging.getLogger(name)
    
    # 避免重複添加 handler
    if logger.handlers:
        return logger
    
    logger.setLevel(getattr(logging, level.upper()))
    
    # 格式化器
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler（可選）
    if log_to_file and log_file_path:
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def log_dataframe_stats(
    logger: logging.Logger,
    df,
    df_name: str,
    show_schema: bool = False
) -> None:
    """
    Log DataFrame statistics.
    
    Args:
        logger: Logger object
        df: PySpark DataFrame
        df_name: DataFrame name
        show_schema: Whether to display schema
    """
    row_count = df.count()
    col_count = len(df.columns)
    
    logger.info(f"DataFrame [{df_name}]: {row_count} rows, {col_count} columns")
    
    if show_schema:
        logger.info(f"Schema: {df.schema.simpleString()}")


def log_pipeline_step(
    logger: logging.Logger,
    step_name: str,
    status: str,
    details: Optional[str] = None,
    duration_seconds: Optional[float] = None
) -> None:
    """
    Log pipeline step execution status.
    
    Args:
        logger: Logger object
        step_name: Step name
        status: Status (START/SUCCESS/FAILED)
        details: Detailed information
        duration_seconds: Execution duration (seconds)
    """
    msg = f"Pipeline Step [{step_name}] - {status}"
    
    if duration_seconds:
        msg += f" - Duration: {duration_seconds:.2f}s"
    
    if details:
        msg += f" - {details}"
    
    if status == "FAILED":
        logger.error(msg)
    else:
        logger.info(msg)


class PipelineTimer:
    """Pipeline timer context manager."""
    
    def __init__(self, logger: logging.Logger, step_name: str):
        self.logger = logger
        self.step_name = step_name
        self.start_time = None
    
    def __enter__(self):
        self.start_time = datetime.now()
        log_pipeline_step(self.logger, self.step_name, "START")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = (datetime.now() - self.start_time).total_seconds()
        
        if exc_type:
            log_pipeline_step(
                self.logger,
                self.step_name,
                "FAILED",
                details=str(exc_val),
                duration_seconds=duration
            )
        else:
            log_pipeline_step(
                self.logger,
                self.step_name,
                "SUCCESS",
                duration_seconds=duration
            )
