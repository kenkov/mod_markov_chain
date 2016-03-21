#! /usr/bin/env python
# coding:utf-8

import random
import sqlite3
from typing import Tuple, Union, List
from collections import defaultdict
import params
import pmi
import langmodel
from langmodel import NotFoundException
import logging
import traceback
import heapq
import math
import datetime


class MarkovChain:
    def __init__(
            self,
            dbname: str
    ):
        self.dbname = dbname
        self.lm = langmodel.TriGram(dbname)
        self.pmi = pmi.PMI(dbname)

    def select_one(
            self,
            words: List[str],
            w1: str,
            w2: str,
            mode: str,
            random_range: int=1
    ) -> str:
        assert mode in {"pmi", "lang", "both"}

        cands = self.lm.cands(w1, w2)
        if not cands:
            raise NotFoundException(
                "no candidates found in select_one:{} {} {}".format(
                    words, w1, w2
                )
            )
        logging.info("w1={}, w2={}, num of candidates: {}, mode:{}".format(
            w1, w2, len(cands), mode
        ))

        pq = [(-float("inf"), ) for _ in range(5)]
        lang_flag = mode in {"lang", "both"}
        pmi_flag = mode in {"pmi", "both"}
        lang_pq = [(-float("inf"), ) for _ in range(5)]
        pmi_pq = [(-float("inf"), ) for _ in range(5)]
        pmi_total = 0

        ts = datetime.datetime.now()
        for i, w3 in enumerate(cands):
            # debug log
            if (i+1) % 100 == 0:
                now = datetime.datetime.now()
                delta = now - ts
                ts = now
                logging.debug("{} prosessed: {}".format(
                    i+1, delta
                ))

            # lang
            lm_prob = self.lm.prob(w1, w2, w3)
            heapq.heappushpop(lang_pq, (lm_prob, w3))
            # PMI
            pmi = sum(self.pmi.pmi(word, w3) for word in words)
            pmi_total += pmi
            heapq.heappushpop(pmi_pq, (pmi, w3))
            # lang * PMI
            heapq.heappushpop(pq, (lm_prob * pmi, w3, lm_prob, pmi))

        if pmi_total == 0:
            logging.info(
                "\tpmi_total = {}: pmi and both mode unavailable".format(
                    pmi_total
                ))
        s_lang_pq = list(
            sorted(
                [item for item in lang_pq if item[0] != -float("inf")],
                reverse=True
            ))
        for lm_prob, w3 in s_lang_pq:
            logging.info("\tP({}|{}, {}) = {:.6}".format(
                w3, w1, w2, lm_prob
            ))
        if pmi_total > 0:
            s_pmi_pq = list(
                sorted(
                    [item for item in pmi_pq if item[0] != -float("inf")],
                    reverse=True
                ))
            for pmi, w3 in s_pmi_pq:
                logging.info("\tPMI({}) = {}: prob {:.6}".format(
                    w3, pmi, pmi / pmi_total
                ))
            spq = list(
                sorted(
                    [item for item in pq if item[0] != -float("inf")],
                    reverse=True
                ))
            for prob, w3, lm_prob, pmi in spq:
                logging.info("\tP({}|{}, {}) = {:.6} = {:.6} * {:.6}".format(
                    w3, w1, w2, prob,
                    lm_prob, prob/pmi_total
                ))
        if mode == "lang" or pmi_total == 0:
            ret_lst = s_lang_pq
        elif mode == "pmi":
            ret_lst = s_pmi_pq
        elif mode == "both":
            ret_lst = spq

        return random.choice(ret_lst[:random_range])[1]

    def generate(
            self,
            words: List[str],
            maxlen: int=30,
            mode: str="both",
            random_range: int=1
    ):
        logging.info("generating sentence in mode: {}".format(mode))
        rep = [params.START_SYMBOL0, params.START_SYMBOL1]
        # rep = [params.START_SYMBOL1, "今日"]
        first_word_flag = True
        while True:
            try:
                next_word = self.select_one(
                    words, rep[-2], rep[-1],
                    mode="pmi" if first_word_flag else mode,
                    random_range=random_range if first_word_flag else 1
                )
                if first_word_flag:
                    first_word_flag = False
                if next_word == params.END_SYMBOL or len(rep) == maxlen:
                    break
                rep.append(next_word)
            except NotFoundException:
                traceback.print_exc()
                break
        return rep[2:]
