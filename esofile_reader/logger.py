import logging

formatter = logging.Formatter("%(name)s - %(levelname)s: %(message)s")
ch = logging.StreamHandler()
ch.setFormatter(formatter)
ch.setLevel(logging.DEBUG)
logger = logging.getLogger(__package__)
logger.addHandler(ch)
logger.setLevel(logging.WARNING)
