
#wrapper exception for use
def exception_handler(func):
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e: 
            raise 
    return wrapper
 
 
