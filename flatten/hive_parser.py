import os
from typing import Dict, List, Union

from lark import Lark, Token, Transformer, Tree
from lark.reconstruct import Reconstructor
from flatten.utils import flatten_dict


class HiveParser:
    def __init__(self) -> None:
        self.grammar = self._load_grammar()
        self.parser = Lark(self.grammar, start="type_db_col")
        self.transformer = SchemaTransformer()

    def __call__(self, hive_str):
        return self.transformer.transform(self.parser.parse(hive_str))

    @staticmethod
    def _load_grammar():
        with open(
            os.path.join(os.path.dirname(__file__), "hive_grammar.lark"), "r"
        ) as f:
            grammar = f.read()
        return grammar


class SchemaTransformer(Transformer):
    def structtype(self, items):
        result = {}
        for item in items:
            if isinstance(item, dict):
                result.update(item)

        return result

    def name_type(self, items):
        return {items[0]: items[1]}

    def NAME(self, items):
        return str(items)

    def primitivetype(self, items):
        if isinstance(items[0], Tree) and len(items) > 1:
            datatype = items.pop(0).data
            return f"{datatype}{''.join(items)}"
        elif isinstance(items[0], Tree):
            return items[0].data
        else:
            raise ValueError("Transformation Error", items)

    def listtype(self, items):
        result = []
        for item in items:
            if not isinstance(item, Tree) and not isinstance(item, Token):
                result.append(item)

        return result


def convert_to_tree(k: str, v: Union[str, List]) -> Tree:
    if isinstance(v, list):
        return Tree(
            "name_type",
            [Token("NAME", k), Tree("primitivetype", [Tree("string", [])])],
        )
    else:
        return Tree(
            "name_type",
            [Token("NAME", k), Tree("primitivetype", [Tree(v, [])])],
        )


def flatten_type(parser, hive_str):
    list_of_columns = []
    transformed = parser(hive_str)
    if isinstance(transformed, dict):
        flattend_tree = flatten_dict(transformed)
        for k, v in flattend_tree.items():
            hive_tree = convert_to_tree(k, v)
            new_hive = Reconstructor(parser.parser).reconstruct(hive_tree)
            list_of_columns.append(new_hive)
    else:
        flattend_tree = transformed
        list_of_columns.append(flattend_tree)

    return list_of_columns