"""
General mock methods.
"""


def mock_func_return(result=None):
    """Return a mock function whose return is the passed kwarg result.

    Parameters
    ----------
    result:
        Object to return

    Returns
    -------
    func:
        returns function whose return is `result`.
    """
    def func(*args, **kwargs):
        """mock function returns result"""
        return result

    return func
