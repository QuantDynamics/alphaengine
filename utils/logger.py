import logging


class StockLogger:
    """
    股票研究日志

    Params:
    name: str, 日志器的名字
    file_name: str, 日志文件名
    file_level: int, 文件的日志等级
    stream_level: int, 日志流的日志等级
    """
    def __init__(
                self, name: str, 
                file_name: str, 
                file_level: int = logging.DEBUG, 
                stream_level: int = logging.INFO):
        self.name = name
        self.file_name = file_name
        self.logger = logging.Logger(name)
        self.file_level = file_level
        self.stream_level = stream_level
        self.add_file_handler()
        self.add_stream_handler()

    def add_file_handler(
                        self,
                        file_format: str = "%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s",
                        mode: str = "w"):
        """
        Param:
        file_format: str, 日志的格式
        mode: str, 文件模式
        """
        fh = logging.FileHandler(self.file_name, mode=mode)
        fh.setFormatter(logging.Formatter(file_format))
        fh.setLevel(self.file_level)
        self.logger.addHandler(fh)

    def add_stream_handler(
                            self,
                            stream_format: str = "%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s"):
        """
        Params:
        stream_format: 日志流的格式
        """
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter(stream_format))
        sh.setLevel(self.stream_level)
        self.logger.addHandler(sh)
