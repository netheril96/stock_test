#!/usr/bin/env python3

import numpy as np
import os
import xlsxwriter
import requests
import sys
import argparse
import logging
import csv
import multiprocessing
import glob


class StockInfo:
    def __init__(self, token):
        self.token = token
        session = requests.Session()
        session.headers['Authorization'] = 'Bearer ' + token
        session.headers['Accept-Encoding'] = 'gzip, deflate'
        self._sess = session

    def get_history(self, sec_id):
        url = 'https://api.wmcloud.com/data/v1//api/equity/getMktEqudCCXE.json'
        r = self._sess.get(url, params={'secID': sec_id})
        return r.json()


def moving_mean(array, window_size):
    cumsum = np.cumsum(array)
    return 1.0 / window_size * (cumsum[window_size:] - cumsum[:-window_size])


def mm_actions(prices, mm):
    assert len(prices) == len(mm)
    result = []
    last_buy = None

    for p, m in zip(prices, mm):
        if p >= m and last_buy is None:
            last_buy = p
        elif p < m and last_buy is not None:
            result.append((last_buy, p))
            last_buy = None
    return np.array(result)


def analyze_actions(actions):
    win_ratios = (actions[:, 1] - actions[:, 0]) / actions[:, 0]
    total_win_ratio = (actions[:, 1] - actions[:, 0]).sum() / actions[:, 0].sum()
    return (len(actions), total_win_ratio, max(0, np.max(win_ratios)), min(0, np.min(win_ratios)),
            np.sum(actions[:, 0] < actions[:, 1]), np.sum(actions[:, 0] > actions[:, 1]))


def parse_csv_file(filename):
    prices = []
    ma20 = []
    with open(filename, encoding='gbk') as f:
        reader = csv.DictReader(f)
        for row in reader:
            stock_id = row['股票代码']
            stock_name = row['股票名称']
            first_date = row['交易日期']
            prices.append(float(row['收盘价']))
            ma20.append(float(row['MA_20']))
    return stock_id, stock_name, first_date, np.array(prices)[::-1], np.array(ma20)[::-1]


def _worker_main(filename):
    stock_id, stock_name, first_date, prices, ma20 = parse_csv_file(filename)
    result = (stock_id, stock_name, first_date) + analyze_actions(mm_actions(prices, ma20))
    return result


def main():
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s %(asctime)s]    %(message)s')
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', required=True, help='输出Excel文件名')
    parser.add_argument('-d', '--dir', required=True, help='CSV文件目录')

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s %(asctime)s]    %(message)s')

    filenames = glob.glob(f'{args.dir}/*.csv')
    logging.info('总共有%d个文件待处理', len(filenames))
    pool = multiprocessing.Pool()
    lazy_results = pool.imap_unordered(_worker_main, filenames, 16)

    writer = xlsxwriter.Workbook(args.file)
    logging.info('打开文件 %s', args.file)
    try:
        percent_format = writer.add_format({'num_format': '0.00%'})
        sheet = writer.add_worksheet()
        for i, name in enumerate(['股票代码', '股票名称', '数据最早日期', '累计次数',
                                  '累计收益率', '单次最大收益率', '单次最大亏损率', '收益次数', '亏损次数']):
            sheet.write_string(0, i, name)

        for j, info in enumerate(lazy_results):
            for i, v in enumerate(info):
                if isinstance(v, str):
                    sheet.write_string(j + 1, i, v)
                elif isinstance(v, (float, np.float32, np.float64)):
                    sheet.write_number(j + 1, i, v, percent_format)
                elif isinstance(v, (int, np.int32, np.int64)):
                    sheet.write_number(j + 1, i, v)
                else:
                    raise AssertionError(f'Invalid value {v} of type {type(v)}')
            if j % 10 == 0:
                logging.info('完成%f%%', (j + 1) / len(filenames) * 100)
        logging.info('完成100%')
    finally:
        writer.close()
        logging.info('关闭文件 %s', args.file)


if __name__ == '__main__':
    main()
