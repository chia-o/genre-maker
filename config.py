"""This file will read the data in database.ini and return a dictionart of configuration parameters."""
from configparser import ConfigParser

def load_config(filename='database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    config = {}
    # checks if database.ini has a postgresql section
    if parser.has_section(section):
        # will return tuples of key-value pairs in section
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception(f"Section {0} not found in the {1} file")
    return config

if __name__ == "__main__":
    config = load_config()
    print(config)