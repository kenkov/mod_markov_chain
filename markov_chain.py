#! /usr/bin/env python
# coding:utf-8

import random
import sqlite3
from typing import Tuple, Union
from collections import defaultdict
import params


class NotFoundException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class MarkovChain:
    def __init__(
            self,
            dbname: str
    ):
        self.dbname = dbname
        self.conn = sqlite3.connect(self.dbname)
        self.cur = self.conn.cursor()

    def select_one(self, query: Union[Tuple[str], Tuple[str, str]]):
        assert len(query) in {1, 2}
        if len(query) == 1:
            search_query = 'select * from chain where w1=?'
        else:
            search_query = 'select * from chain where w1=? and w2=?'

        self.cur.execute(search_query, query)
        items = self.cur.fetchall()

        if not items:
            raise NotFoundException(
                "next words not found in {}".format(self.dbname)
            )

        cands = defaultdict(int)
        for coll in items:
            # w2 or w3 の count を足す
            #   cands[w2] += count if len(query) == 1
            #   cands[w3] += count if len(query) == 3
            cands[coll[-2]] += coll[-1]

        return random.choice(
            sum([[word] * cnt for word, cnt in cands.items()], [])
        )

    def generate(
            self,
            query: Union[Tuple[str], Tuple[str, str]],
            maxlen: int=40
    ):
        head_query = query
        words = list(query)
        while True:
            try:
                next_word = self.select_one(head_query)
                if next_word == params.END_SYMBOL or len(words) == maxlen:
                    break
                words.append(next_word)
                head_query = words[-2:]
            except NotFoundException:
                break
        return words


if __name__ == "__main__":

    def insert(cur, query):
        cur.execute(
            'select * from chain where w1=? and w2=? and w3=?',
            query
        )
        items = cur.fetchall()

        if len(items) > 1:
            assert("items duplicated")

        if items:
            count = items[0][3]
            cur.execute(
                "update chain set count=? where w1=? and w2=? and w3=?",
                (count + 1, ) + query
            )
        else:
            cur.execute(
                "insert into chain values (?,?,?,?)",
                query + (1,)
            )

    from argparse import ArgumentParser
    import logging

    # logging config
    logging.basicConfig(
        level=logging.INFO
    )

    p = ArgumentParser(
        description='Markov Chain DB generator',
    )
    p.add_argument('db', help='database name')
    p.add_argument('filename', help='corpus file name')
    args = p.parse_args()

    conn = sqlite3.connect(args.db)
    cur = conn.cursor()
    conn.execute((
        "create table chain"
        "(w1 text, w2 text, w3 text, count int)"
    ))
    conn.execute((
        "create index index_chain_w1_w2_w3 on chain"
        "(w1, w2, w3)"
    ))
    conn.execute((
        "create index index_chain_w1_w2 on chain"
        "(w1, w2)"
    ))
    conn.execute((
        "create index index_chain_w1 on chain"
        "(w1)"
    ))

    with open(args.filename) as fd:
        for i, line in enumerate(_.strip() for _ in fd):
            words = (
                [params.START_SYMBOL] +
                line.split() +
                [params.END_SYMBOL]
            )
            for query in zip(words, words[1:], words[2:]):
                insert(cur, query)
            if (i + 1) % 10000 == 0:
                logging.info("{} lines processed".format(i + 1))
    conn.commit()
    conn.close()
