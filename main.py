
def get_root_path():
    """

    :rtype : object
    """
    import os
    path = os.path.dirname(__file__)
    path = path.replace('\\', '/')
    return path


if __name__ == '__main__':
    pass
