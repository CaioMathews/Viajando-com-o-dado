"""As descrições apontam para etapas, funções e arquivos reais."""

import ast
from pathlib import Path
import unittest

from pipeline.shared.catalog import catalog


ROOT = Path(__file__).resolve().parents[1]


class CatalogContextTests(unittest.TestCase):
    def test_every_node_identifies_its_check_stage(self):
        for node in catalog():

            with self.subTest(node=node["id"]):

                self.assertTrue(node["stage_name"])
                self.assertTrue(node["checks"])

                if node["layer"] != "source":
                    self.assertTrue(node["check_function"])

    def test_method_definitions_and_callers_exist(self):
        for node in catalog():

            for method in node["methods"]:

                with self.subTest(node=node["id"], method=method["name"]):

                    self.assertEqual(method["stage"], node["stage_name"])
                    self.assertTrue(method["role"])
                    self.assertTrue(method["usage"])
                    references = [(method["file"], method["name"])]

                    for caller in method["used_by"].split(" → "):

                        file, _, function = caller.partition(" :: ")
                        references.append((file, function))

                    for file, function in references:

                        source = (ROOT / file).read_text(encoding="utf-8-sig")
                        tree = ast.parse(source)
                        names = set()

                        for item in ast.walk(tree):

                            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                                names.add(item.name)

                        self.assertIn(function, names, file)

    def test_manual_sources_do_not_claim_an_automatic_collection_method(self):
        for node in catalog():

            if node["id"] in {"source:population", "source:inmet_daily", "source:inmet_boundary"}:
                self.assertEqual(node["methods"], [])
