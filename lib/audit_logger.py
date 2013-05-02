import logging

default_formatter = logging.Formatter(
    "%(asctime)s %(name)s[%(process)s]: [%(levelname)s] %(message)s",
    "%b %d %H:%M:%S")

handler = logging.FileHandler('logs/audit.log')
handler.setFormatter(default_formatter)

audit_logger = logging.getLogger('ingestion')
audit_logger.addHandler(handler)
audit_logger.setLevel(logging.DEBUG)
