#! /usr/bin/env python
# coding:utf-8


from mod import Mod
import random
from head import HeadSelector
from markov_chain import MarkovChain
import params


class ModMarkovChain(Mod):
    def __init__(
            self,
            markov_chain_db: str,
            head_db: str,
            pmi_db: str,
            logger=None
    ):
        Mod.__init__(self, logger)
        self.markov_chain_db = markov_chain_db
        self.head_db = head_db
        self.gen = MarkovChain(self.markov_chain_db)
        self.hs = HeadSelector(self.head_db, pmi_db)

    def gen_from_sentence(self, sent, num=5):
        heads = self.hs.select(sent, num=num)
        print(heads)
        replies = []
        for head, score in heads:
            query = (params.START_SYMBOL, head, )
            replies.append(self.gen.generate(query))
        return ["".join(_[1:]) for _ in replies]

    def can_utter(self, message, master):
        return True

    def utter(self, message, master):
        return [
            (random.uniform(0.7, 1.0),
             text, "markov_chain", dict())
            for text in self.gen_from_sentence(
                    message["text"],
                    num=3
            )
        ]
