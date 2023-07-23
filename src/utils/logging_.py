import logging


def get_logger(debug: bool,
               silent: bool):

    LEVEL = 20

    if debug:
        LEVEL = 10

    if silent:
        LEVEL = 60

    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%d-%b-%y %H:%M:%S',
                        level=LEVEL
                        )

    return logging.getLogger('dockerization_logger')