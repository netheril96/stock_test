#!/usr/bin/env python3

import numpy as np
import os
import xlsxwriter
import requests
import sys
import argparse
import logging


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
    return result


def compute(stock_info, stock_id, avg_window=20, print_actions=False, plot_prices=False):
    sec_id = stock_id
    if stock_id[0] == '0':
        sec_id += '.XSHE'
    elif stock_id[0] == '6':
        sec_id += '.XSHG'
    else:
        raise ValueError(f'Invalid stock id {stock_id}')
    hist = stock_info.get_history(sec_id)
    try:
        hist = hist['data']
    except KeyError:
        raise RuntimeError(hist['retMsg'])
    hist.sort(key=lambda h: h['tradeDate'])
    prices = np.array([h['closePrice'] for h in hist])
    mm = moving_mean(prices, avg_window)
    actions = np.array(mm_actions(prices[avg_window:], mm))

    win_ratios = (actions[:, 1] - actions[:, 0]) / actions[:, 0]
    total_win_ratio = (actions[:, 1] - actions[:, 0]).sum() / actions[:, 0].sum()
    if print_actions:
        print('actions:', actions, 'win_ratios', win_ratios, sep='\n')

    if plot_prices:
        from matplotlib import pyplot as plt
        plt.cla()
        plt.plot(prices[avg_window:], label='Close prices', marker='o')
        plt.plot(mm, label=f'M{avg_window}', marker='o')
        plt.legend()
        plt.show()

    return (stock_id, hist[0]['secShortName'], hist[0]['tradeDate'], len(actions),
            total_win_ratio, max(0, np.max(win_ratios)),
            min(0, np.min(win_ratios)), np.sum(actions[:, 0] < actions[:, 1]),
            np.sum(actions[:, 0] > actions[:, 1]))


def main():
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s %(asctime)s]    %(message)s')
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--id', required=True, help='一个或多个股票代码，用逗号分离')
    parser.add_argument('-f', '--file', required=True, help='输出Excel文件名')
    parser.add_argument('-t', '--token', required=True, help='通联数据Token')
    parser.add_argument('--plot', action='store_true', help='显示股票走势')

    args = parser.parse_args()
    ids = args.id.split(',')
    stock_info = StockInfo(args.token)
    writer = xlsxwriter.Workbook(args.file)
    try:
        percent_format = writer.add_format({'num_format': '0.00%'})
        sheet = writer.add_worksheet()
        for i, name in enumerate(['股票代码', '股票名称', '数据最早日期', '累计次数',
                                  '累计收益率', '单次最大收益率', '单次最大亏损率', '收益次数', '亏损次数']):
            sheet.write_string(0, i, name)

        for j, id in enumerate(ids):
            logging.info('获取股票%s数据中...', id)
            try:
                info = compute(stock_info=stock_info, stock_id=id, plot_prices=args.plot)
            except Exception as e:
                logging.error('异常%r', e)
            else:
                logging.info('写入文件%s', args.file)
                for i, v in enumerate(info):
                    if isinstance(v, str):
                        sheet.write_string(j + 1, i, v)
                    elif isinstance(v, (float, np.float32, np.float64)):
                        sheet.write_number(j + 1, i, v, percent_format)
                    elif isinstance(v, (int, np.int32, np.int64)):
                        sheet.write_number(j + 1, i, v)
                    else:
                        raise AssertionError(f'Invalid value {v} of type {type(v)}')
    finally:
        writer.close()
        logging.info('关闭文件%s', args.file)


if __name__ == '__main__':
    main()
