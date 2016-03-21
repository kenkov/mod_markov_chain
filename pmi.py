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
        self.num_s = self.cur.execute(
            'select sum(count) from s_pmi'
        ).fetchone()[0]
        self.num_t = self.cur.execute(
            'select sum(count) from t_pmi'
        ).fetchone()[0]
        self.num_s_plus_t = self.num_s + self.num_t
        logging.info("PMI info: num_s_t={}, num_s={}, num_t={}".format(
            self.num_total, self.num_s, self.num_t
        ))

    def pmi(self, s, t):

        self.cur.execute(
            'select count from s_pmi where s=?',
            (s, )
        )
        _num_s = self.cur.fetchall()
        self.cur.execute(
            'select count from t_pmi where t=?',
            (t, )
        )
        _num_t = self.cur.fetchall()
        self.cur.execute(
            'select count from pmi where s=? and t=?',
            (s, t)
        )
        _num_s_t = self.cur.fetchall()

        has_val = _num_s and _num_t and _num_s_t
        if has_val:
            assert len(_num_s) == 1
            assert len(_num_t) == 1
            assert len(_num_s_t) == 1
            num_s = _num_s[0][0]
            num_t = _num_t[0][0]
            num_s_t = _num_s_t[0][0]
            # logging.debug("{} {} {}".format(num_s_t, num_s, num_t))
            # 低頻度バイアスに対応した PPMI 計算
            pmi_val = (
                math.log(num_s_t) - (math.log(num_s) + math.log(num_t)) +
                (math.log(self.num_s) + math.log(self.num_t) -
                 math.log(self.num_total))
            ) * (
                (num_s_t / (num_s_t + 1)) *
                min(num_s, num_t) / (min(num_s, num_t) + 1)
            )
        else:
            pmi_val = 0
            # logging.debug("PMI key error: {} {}".format(s, t))
        return pmi_val if pmi_val >= 0 else 0


if __name__ == '__main__':
    from argparse import ArgumentParser

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

    from collections import defaultdict
    s_dic = defaultdict(int)
    t_dic = defaultdict(int)
    s_t_dic = defaultdict(int)

    with open(args.filename) as fd:
        for i, line in enumerate(_.strip() for _ in fd):
            orig, reply = line.split("\t")
            orig_words = (
                [params.START_SYMBOL0, params.START_SYMBOL1] +
                orig.split() +
                [params.END_SYMBOL]
            )
            reply_words = (
                [params.START_SYMBOL0, params.START_SYMBOL1] +
                reply.split() +
                [params.END_SYMBOL]
            )
            # for orig_word in orig_words:
            #     s_dic[orig_word] += 1
            # for reply_word in reply_words:
            #     t_dic[reply_word] += 1
            for orig_word in orig_words:
                for reply_word in reply_words:
                    s_t_dic[(orig_word, reply_word)] += 1
                    s_dic[orig_word] += 1
                    t_dic[reply_word] += 1
            if (i + 1) % 10000 == 0:
                logging.info("{} lines processed".format(i + 1))
    # conn.commit()
    # conn.close()

    for line in (
        "create table pmi (s text, t text, count int);",
        "create index pmi_s_t on pmi (s, t);",
        "create table s_pmi (s text, count int);",
        "create index s_pmi_s on s_pmi (s);",
        "create table t_pmi (t text, count int);",
        "create index t_pmi_t on t_pmi (t);",
    ):
        print(line)

    for (s, t), count in s_t_dic.items():
        print(
            'insert into pmi values ("{}","{}",{});'.format(s, t, count)
        )
    for s, count in s_dic.items():
        print(
            'insert into s_pmi values ("{}",{});'.format(s, count)
        )
    for t, count in t_dic.items():
        print(
            'insert into t_pmi values ("{}",{});'.format(t, count)
        )
