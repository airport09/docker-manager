import botocore

def credentials_not_found(f):

    def applicator(*args, **kwargs):
        logger = args[0].logger
        try:
            return f(*args,**kwargs)
        except botocore.exceptions.NoCredentialsError:
            logger.critical('AWS CREDENTIALS NOT FOUND. EXITING...')
            exit()

    return applicator

