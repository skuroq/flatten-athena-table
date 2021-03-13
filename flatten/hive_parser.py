import os
from typing import List
from typing import Union

from flatten.utils import flatten_dict
from lark import Lark
from lark import Token
from lark import Transformer
from lark import Tree
from lark.reconstruct import Reconstructor


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


def reconstruct_primitive_type(k, v):
    return Tree(
        "name_type",
        [
            Token("NAME", k),
            Tree("primitivetype", [Tree(v, [])]),
        ],
    )


def reconstruct_struct_type(type: dict):
    tree = Tree("structtype", [])
    for k, v in type.items():
        if isinstance(v, dict):
            tree.children.append(
                Tree("name_type", [Token("NAME", k), reconstruct_struct_type(v)])
            )
        else:
            tree.children.append(reconstruct_primitive_type(k, v))
    return tree


def reconstruct_array_type(type: List):
    tree = Tree("listtype", [])
    for i in type:
        if isinstance(i, dict):
            # for example array<struct<a:string,b:string>>
            tree.children.append(reconstruct_struct_type(i))
        else:
            # for example array<string>
            tree.children.append(
                Tree("primitivetype", [Tree(i, [])]),
            )
    return tree


def reconstruct_array(parser, type: List):
    tree = reconstruct_array_type(type)
    return Reconstructor(parser).reconstruct(tree)
