#! /usr/bin/env python
# coding:utf-8

import sqlite3
from cabocha import CaboChaAnalyzer
import logging
from pmi import PMI
import params
from typing import Tuple


class HeadSelector:
    def __init__(
            self,
            dbname: str,
            pmi_db: str
    ):
        self.dbname = dbname
        self.conn = sqlite3.connect(dbname)
        self.cur = self.conn.cursor()
        self.analyzer = CaboChaAnalyzer(
            params.CABOCHA_OPTION
        )
        self.pmi = PMI(pmi_db)

    def _select(
            self,
            features,
            num: int=5
    ):

        search_query = 'select * from head_rel where s=?'

        heads = set()

        # feature の少なくともひとつと共起する
        # head word を取得
        for word in features:
            query = (word, )

            self.cur.execute(search_query, query)
            items = self.cur.fetchall()
            for coll in items:
                heads.add(coll[-2])

        # if found nothing, raise error
        if not heads:
            return []
        else:
            head_score = dict()
            for head in heads:
                score = 0
                for word in features:
                    score += self.pmi.pmi(word, head)
                head_score[head] = score
                # print("{}: {}".format(head, score))

            return sorted(
                head_score.items(),
                key=lambda _: _[1],
                reverse=True
            )[:num]

    def select(self, sent: str, num: int =5) -> Tuple[str, float]:
        # parse
        tree = self.analyzer.parse(sent)
        tokens = sum(
            [[token for token in chunk] for chunk in tree],
            []
        )
        features = set()

        for token in tokens:
            # if (token.pos, token.pos1) in [
            #         ("動詞", "自立"),
            #         ("名詞", "一般"),
            #         ("名詞", "固有名詞"),
            #         ("名詞", "副詞可能"),
            # ]:
            if token.pos in params.POS_FILTER:
                features.add(token.surface)
        logging.info("features: {}".format(features))
        heads = self._select(features, num)
        return heads

if __name__ == "__main__":

    def insert(cur, query):
        cur.execute(
            'select * from head_rel where s=? and t=?',
            query
        )
        items = cur.fetchall()

        if len(items) > 1:
            assert("items duplicated")

        if items:
            count = items[0][2]
            cur.execute(
                "update head_rel set count=? where s=? and t=?",
                (count + 1, ) + query
            )
        else:
            cur.execute(
                "insert into head_rel values (?,?,?)",
                query + (1,)
            )
    from argparse import ArgumentParser

    # logging config
    logging.basicConfig(
        level=logging.INFO
    )

    p = ArgumentParser(
        description='pair generator',
    )
    p.add_argument('db', help='database name')
    p.add_argument('filename', help='corpus file name')
    args = p.parse_args()

    analyzer = CaboChaAnalyzer(params.CABOCHA_OPTION)

    # setup db
    conn = sqlite3.connect(args.db)
    cur = conn.cursor()
    conn.execute((
        "create table head_rel"
        "(s text, t text, count int)"
    ))
    conn.execute((
        "create index index_s_t on head_rel"
        "(s, t)"
    ))
    conn.execute((
        "create index index_s on head_rel"
        "(s)"
    ))

    with open(args.filename) as fd:
        for i, line in enumerate(_.strip() for _ in fd):
            orig, reply = line.split("\t")
            # parse
            orig_tree = analyzer.parse(orig)
            reply_tree = analyzer.parse(reply)
            orig_tokens = sum(
                [[token for token in chunk] for chunk in orig_tree],
                []
            )
            reply_tokens = sum(
                [[token for token in chunk] for chunk in reply_tree],
                []
            )

            reply_head = reply_tokens[0].surface
            for token in orig_tokens:
                if token.pos in params.POS_FILTER:
                    insert(cur, (token.surface, reply_head))
            if (i + 1) % 10000 == 0:
                logging.info("{} lines processed".format(i + 1))
    conn.commit()
    conn.close()
