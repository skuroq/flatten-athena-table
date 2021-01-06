from slugify import slugify
from functools import lru_cache
from typing import Dict


@lru_cache(maxsize=1024)
def slugify_key(
    key,
    subkey=None,
    separator="_",
):

    if subkey:
        path = key + separator + subkey
    else:
        path = key
    path = slugify(
        path,
        lowercase=True,
        separator=separator,
        replacements=[[" ", ""]],
    )
    return path


def column_query_path_format(
    key,
    subkey=None,
    separator=".",
):
    key = f'"{key}"'
    if subkey:

        path = key + separator + subkey
    else:
        path = key
    return path


def flatten_dict(
    d: Dict,
    format_func=slugify_key,
) -> Dict:
    """
    Flattens a given dict
    :param d:
    :param seperator:
    :return:
    """

    def items():
        for key, value in d.items():
            if isinstance(value, dict):
                for subkey, subvalue in flatten_dict(
                    value, format_func=format_func
                ).items():
                    yield format_func(key=key, subkey=subkey), subvalue
            else:
                yield format_func(key=key), value

    return dict(items())
