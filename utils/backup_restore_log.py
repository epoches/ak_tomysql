import os
from inspect import stack


def get_caller_info():
    """Get the calling function's name and directory."""
    caller_frame = stack()[2]  # Get the frame of the caller's caller
    caller_function = caller_frame.function
    caller_file = caller_frame.filename
    caller_dir = os.path.dirname(os.path.abspath(caller_file))
    return caller_function, caller_dir


def load_processed_codes():
    """Load previously processed codes from a file named after the calling function."""
    caller_function, caller_dir = get_caller_info()
    log_file = os.path.join(caller_dir, f"{caller_function}_processed_codes.txt")

    if os.path.exists(log_file):
        with open(log_file, 'r') as f:
            return set(f.read().splitlines())
    return set()


def save_processed_code(code):
    """Append a successfully processed code to a file named after the calling function."""
    caller_function, caller_dir = get_caller_info()
    log_file = os.path.join(caller_dir, f"{caller_function}_processed_codes.txt")

    with open(log_file, 'a') as f:
        f.write(str(code) + '\n')


def clear_processed_codes():
    """Clear the processed codes file after successful completion."""
    caller_function, caller_dir = get_caller_info()
    log_file = os.path.join(caller_dir, f"{caller_function}_processed_codes.txt")

    if os.path.exists(log_file):
        os.remove(log_file)