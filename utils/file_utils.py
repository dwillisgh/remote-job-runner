import os


def create_directories(path):
    if not os.path.exists(path):
        os.makedirs(path)


def write_buffer_to_file(path, filename, buffer):
    # causes ~ to be converted to real path on the mac
    path = os.path.expanduser(path)
    create_directories(path)

    with open(os.path.join(path, filename), 'w') as temp_file:
        temp_file.write(buffer)


def replace_special_chars_to_underscore(fromstr, truncation_size: int):
    fromstr = fromstr.replace(" ", "_")
    fromstr = fromstr.replace("(", "_")
    fromstr = fromstr.replace(")", "_")
    fromstr = fromstr.replace("{", "_")
    fromstr = fromstr.replace("}", "_")
    fromstr = fromstr.replace(".", "_")
    fromstr = fromstr.replace(",", "_")
    fromstr = fromstr.replace("[", "_")
    fromstr = fromstr.replace("]", "_")
    fromstr = fromstr.replace("!", "_")
    fromstr = fromstr.replace(";", "_")
    fromstr = fromstr.replace("\"", "_")
    fromstr = fromstr.replace("\'", "_")
    fromstr = fromstr.replace("*", "_")
    fromstr = fromstr.replace("?", "_")
    fromstr = fromstr.replace("<", "_")
    fromstr = fromstr.replace(">", "_")
    fromstr = fromstr.replace("|", "_")
    fromstr = fromstr.replace("/", "_")
    fromstr = fromstr.replace("\\", "_")
    fromstr = fromstr[0: truncation_size]

    return fromstr


def list_directory_files(path):
    path = os.path.expanduser(path)
    return os.listdir(path)