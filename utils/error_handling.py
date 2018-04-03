
"""Error handling utils."""
import sys
import traceback
from post_rocketchat_message import post_rocketchat_message


def _get_error_message_details(error):
    """Nicely extract error text and traceback."""
    error_traceback = sys.exc_info()[-1]
    stk = traceback.extract_tb(error_traceback, 1)

    return (
        '{error_class} processing TIN / NPI message! '
        'In {function}, the following error happened - {detail} at line {line}. '
    ).format(
        error_class=error.__class__.__name__,
        function=stk[0][2],
        detail=error.args[0],
        line=stk[-1][1]
    )


def send_error(error, code_file):
    """Send error message to logger and Slack."""
    error_msg = _get_error_message_details(error)
    #logging.error(error_msg)
    # send mail to mapswipe google group with
    print(error_msg)
    error_msg = _get_error_message_details(error)
    head = 'mapswipe-analytics-backend: {}: error occured'.format(code_file)
    post_rocketchat_message('../cfg/config.cfg', head + '\n' + error_msg)