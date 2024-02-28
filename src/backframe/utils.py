from functools import wraps
from typing import Optional, get_type_hints
from fastapi import Response

from pydantic import TypeAdapter


def fix_response(fn):
    """
    XXX: Arregla un mal comportamiento de fastapi por el cual no serializa adecuadamente
    el valor de salida.
    """
    type_adapter: Optional[TypeAdapter] = None

    if "return" not in fn.__annotations__:
        raise TypeError(f'fix_response: "{fn.__name__}" must have a return annotation')

    @wraps(fn)
    async def wrapper(*args, **kwargs):
        nonlocal type_adapter

        result = await fn(*args, **kwargs)

        if type_adapter is None:
            fn_hint = get_type_hints(fn)
            type_adapter = TypeAdapter(fn_hint.get("return"))

        return Response(type_adapter.dump_json(result))

    return wrapper
