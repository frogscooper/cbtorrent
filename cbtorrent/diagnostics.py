"""Readable error text for bounded reports, and quiet handling of peer resets."""
import asyncio

_RESETS = (ConnectionResetError, ConnectionAbortedError, BrokenPipeError)


def describe_error(error):
    """One line that never comes out empty; timeouts often have no message."""
    if isinstance(error, asyncio.IncompleteReadError):
        return (f"connection closed by peer ({len(error.partial)} of "
                f"{error.expected} bytes read)")
    text = str(error).strip()
    if isinstance(error, TimeoutError):
        return f"timed out ({text})" if text else "timed out"
    return text or type(error).__name__


def install_reset_filter(loop=None):
    """Keep remote TCP resets during transport teardown out of stderr.

    On Windows, the Proactor loop reports a peer that resets a closing socket
    (WinError 10054) as "Exception in callback ..._call_connection_lost" with a
    traceback. The connection is already gone and its owner has already seen
    the failure, so nothing is lost. Everything else, including unretrieved
    task exceptions, still reaches the previous handler. Installing twice is
    harmless; the filter stays on the loop because sessions can overlap.
    """
    loop = loop or asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    if getattr(previous, "cbtorrent_reset_filter", False):
        return

    def handler(loop, context):
        if isinstance(context.get("exception"), _RESETS) and "future" not in context:
            callback = getattr(context.get("handle"), "_callback", None)
            if ("_call_connection_lost" in context.get("message", "")
                    or getattr(callback, "__name__", "") == "_call_connection_lost"):
                return
        if previous is not None:
            previous(loop, context)
        else:
            loop.default_exception_handler(context)

    handler.cbtorrent_reset_filter = True
    loop.set_exception_handler(handler)
