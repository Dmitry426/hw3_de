import os

def load_sql_script(file_path: str) -> str:
    """
    Load the SQL script from the given file path.

    :param file_path: Path to the SQL file.
    :return: The contents of the SQL file as a string.
    :raises FileNotFoundError: If the file does not exist.
    :raises PermissionError: If there is no permission to read the file.
    :raises IOError: For other IO-related issues.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    if not os.access(file_path, os.R_OK):
        raise PermissionError(f"No read permission for file: {file_path}")

    try:
        with open(file_path, 'r') as file:
            return file.read()
    except UnicodeDecodeError:
        raise ValueError(f"The file is not a valid text file: {file_path}")
    except IOError as e:
        raise IOError(f"An error occurred while reading the file: {file_path}. Error: {e}")
