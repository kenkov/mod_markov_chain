#! /usr/bin/env python
# coding:utf-8

import math
import sqlite3
import params
import logging


class PMI:
    def __init__(
            self,
            dbname: str
    ):
        self.dbname = dbname
        self.conn = sqlite3.connect(self.dbname)
        self.cur = self.conn.cursor()

        # calculate the number of atterance of words
        self.num_total = self.cur.execute(
            'select sum(count) from pmi'
        ).fetchone()[0]

    def pmi(self, s, t):

        num_s = self.cur.execute(
            'select sum(count) from pmi where s=?',
            (s, )
        ).fetchone()[0]
        num_t = self.cur.execute(
            'select sum(count) from pmi where t=?',
            (t, )
        ).fetchone()[0]
        num_s_t = self.cur.execute(
            'select sum(count) from pmi where s=? and t=?',
            (s, t)
        ).fetchone()[0]
        logging.debug("{} {} {}".format(num_s_t, num_s, num_t))

        has_val = num_s and num_t and num_s_t
        if has_val:
            # 低頻度バイアスに対応した PPMI 計算
            pmi_val = (
                math.log(num_s_t)
                - math.log(num_s)
                - math.log(num_t)
                + math.log(self.num_total)
            ) * (
                (num_s_t / (num_s_t + 1)) *
                min(num_s, num_t) / (min(num_s, num_t) + 1)
            )
        else:
            pmi_val = 0
            logging.debug(
                "PMI key error: num_{}={} num_{}={} num_{}_{}={}".format(
                    s, num_s, t, num_t, s, t, num_s_t
                ))
        return pmi_val if pmi_val >= 0 else 0


if __name__ == '__main__':
    from argparse import ArgumentParser

    def insert(cur, query):
        cur.execute(
            'select * from pmi where s=? and t=?',
            query
        )
        items = cur.fetchall()

        if len(items) > 1:
            assert("items duplicated")

        if items:
            count = items[0][2]
            cur.execute(
                "update pmi set count=? where s=? and t=?",
                (count + 1, ) + query
            )
        else:
            cur.execute(
                "insert into pmi values (?,?,?)",
                query + (1,)
            )

    # logging config
    logging.basicConfig(
        level=logging.INFO
    )

    p = ArgumentParser(
        description='PMI',
    )
    p.add_argument('db', help='database name')
    p.add_argument('filename', help='corpus file name')
    args = p.parse_args()

    # setup db
    conn = sqlite3.connect(args.db)
    cur = conn.cursor()
    conn.execute((
        "create table pmi"
        "(s text, t text, count int)"
    ))
    conn.execute((
        "create index index_s_t on pmi"
        "(s, t)"
    ))
    conn.execute((
        "create index index_s on pmi"
        "(s)"
    ))
    conn.execute((
        "create index index_t on pmi"
        "(t)"
    ))

    with open(args.filename) as fd:
        for i, line in enumerate(_.strip() for _ in fd):
            orig, reply = line.split("\t")
            orig_words = (
                [params.START_SYMBOL] +
                orig.split() +
                [params.END_SYMBOL]
            )
            reply_words = (
                [params.START_SYMBOL] +
                reply.split() +
                [params.END_SYMBOL]
            )

            for orig_word in orig_words:
                for reply_word in reply_words:
                    insert(cur, (orig_word, reply_word))
            if (i + 1) % 10000 == 0:
                logging.info("{} lines processed".format(i + 1))
    conn.commit()
    conn.close()
