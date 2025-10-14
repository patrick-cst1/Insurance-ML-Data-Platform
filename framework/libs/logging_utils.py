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
    獲取統一格式嘅 logger。
    
    Args:
        name: Logger 名稱
        level: 日誌級別（DEBUG/INFO/WARNING/ERROR）
        log_to_file: 是否寫入文件
        log_file_path: 日誌文件路徑
    
    Returns:
        Logger 對象
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
    記錄 DataFrame 統計信息。
    
    Args:
        logger: Logger 對象
        df: PySpark DataFrame
        df_name: DataFrame 名稱
        show_schema: 是否顯示 schema
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
    記錄 pipeline 步驟執行狀態。
    
    Args:
        logger: Logger 對象
        step_name: 步驟名稱
        status: 狀態（START/SUCCESS/FAILED）
        details: 詳細信息
        duration_seconds: 執行時長（秒）
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
    """Pipeline 計時器上下文管理器。"""
    
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
