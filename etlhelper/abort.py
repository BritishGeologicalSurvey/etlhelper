"""
Functions used to handle aborts during threaded operations.
"""
import threading

from etlhelper.exceptions import ETLHelperAbort

abort_event = threading.Event()


def abort_etlhelper_threads():
    """
    Abort the ETLHelper process at the end of the current chunk.  During
    threaded operation, this will cause an ETLHelperAbort error to be
    raised.
    """
    abort_event.set()


def clear_abort_event():
    """Clear abort_event."""
    abort_event.clear()


def raise_for_abort(message):
    """Raise EtlHelperAbort exception with message if abort_event is set."""
    if abort_event.is_set():
        raise ETLHelperAbort(message)
