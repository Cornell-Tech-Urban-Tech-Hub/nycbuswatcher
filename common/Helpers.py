#-------------- Pretty JSON -------------------------------------------------------------
# https://gitter.im/tiangolo/fastapi?at=5d381c558fe53b671dc9aa80
import json
import typing
from starlette.responses import Response

class PrettyJSONResponse(Response):
    media_type = "application/json"

    def render(self, content: typing.Any) -> bytes:
        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            indent=4,
            separators=(", ", ": "),
        ).encode("utf-8")


#-------------- timer decorator -------------------------------------------------------------
import functools
from time import perf_counter, strftime

def timer(func):
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        tic = perf_counter()
        value = func(*args, **kwargs)
        toc = perf_counter()
        elapsed_time = toc - tic
        logging.warning(f"{func.__name__!r} finished at {strftime('%l:%M%p %Z on %b %d, %Y') } in {elapsed_time:0.4f} seconds")
        return value
    return wrapper_timer
#--------------------------------------------------------------------------------------------------
