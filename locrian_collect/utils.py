"""
General utility methods.
"""
import getpass
import json
import os


def set_db_login():
    """Save the database login credentials to a file in the users home directory.

    Note this saves credentials unencrypted.
    """
    home = os.path.expanduser('~')

    print('Enter db credentials below.  Note this will save the db password unencrytped '
          'in the users home directory in `.db_login`')
    username = str(input("Enter Database Username: "))
    password = getpass.getpass(prompt="Enter Database Password: ")

    config = json.dumps({'username': username, 'password': password})

    with open(f'{home}/.db_login', 'w') as f:
        f.write(config)


def get_db_login():
    """Get the stored database credentials from a file.

    Returns
    -------
    tuple
        Tuple of the username and password.
    """
    with open(f'{os.path.expanduser("~")}/.db_login', 'r') as f:
        credentials = json.loads(f.read())

    return credentials['username'], credentials['password']
