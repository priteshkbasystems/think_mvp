import json
import os

LEXICON_PATH = "data/transformation_lexicon.json"


class TransformationLexicon:

    def __init__(self):

        if not os.path.exists(LEXICON_PATH):
            raise Exception("Transformation lexicon not found")

        with open(LEXICON_PATH, "r") as f:
            self.lexicon = json.load(f)

    def get_terms(self):

        return self.lexicon.get("transformation_terms", [])

    def expand_topics(self, topics):

        expanded = set(topics)

        for term in self.get_terms():
            expanded.add(term)

        return list(expanded)