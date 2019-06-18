# """Test logging"""
# import tempfile
#
# from locrian_collect.logs import get_logger
#
#
# def test_get_logger():
#     """Test get_logger returns the expected logger for two calls."""
#
#     with tempfile.NamedTemporaryFile() as temp:
#         expected_dir = temp.name
#         expected_fmt = '[%(asctime)s : %(name)s : %(levelname)s]  %(message)s'
#
#         for _ in range(2):
#             logger = get_logger(temp.name, 'test_name')
#
#             assert logger.name == 'test_name'
#
#             assert logger.handlers[0].__dict__['baseFilename'] == expected_dir
#             assert logger.handlers[1].formatter._fmt == expected_fmt
