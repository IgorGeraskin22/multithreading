# -*- coding: utf-8 -*-

import pandas as pd
import os
from pathlib import Path
import operator
from threading import Thread
import threading


# Задача: вычислить 3 тикера с максимальной и 3 тикера с минимальной волатильностью в МНОГОПОТОЧНОМ стиле
#
# Бумаги с нулевой волатильностью вывести отдельно.
# Результаты вывести на консоль в виде:
#   Максимальная волатильность:
#       ТИКЕР1 - ХХХ.ХХ %
#       ТИКЕР2 - ХХХ.ХХ %
#       ТИКЕР3 - ХХХ.ХХ %
#   Минимальная волатильность:
#       ТИКЕР4 - ХХХ.ХХ %
#       ТИКЕР5 - ХХХ.ХХ %
#       ТИКЕР6 - ХХХ.ХХ %
#   Нулевая волатильность:
#       ТИКЕР7, ТИКЕР8, ТИКЕР9, ТИКЕР10, ТИКЕР11, ТИКЕР12
# Волатильности указывать в порядке убывания. Тикеры с нулевой волатильностью упорядочить по имени.
#

class Ticker(Thread):
    def __init__(self, full_path):
        super().__init__()
        self.full_path = full_path
        self.volatility = None
        self.name_ticker = None

    def run(self):
        try:
            self.volatility_calculation()
        except Exception as exc:
            print(exc)

    def volatility_calculation(self):
        frame_price = pd.read_csv(self.full_path)
        self.name_ticker = frame_price['SECID'][0]
        max_price = frame_price['PRICE'].max()
        min_price = frame_price['PRICE'].min()
        half_sum = (max_price + min_price) / 2
        self.volatility = round(((max_price - min_price) / half_sum) * 100, 2)


class Data:
    zero_volatility = []  # бумаги с нулевой волотильностью
    ticker_dictionary = {}
    volatility_dict = {}

    def __init__(self, my_path):
        self.ticker_file = None
        self.address = None
        self.files = None
        self.my_path = my_path
        self.ticker = None

    def data_output(self):
        threas = []
        for self.address, self.dirs, self.files in os.walk(self.my_path):
            for self.ticker_file in self.files:
                self.ticker = Ticker(os.path.join(self.address, self.ticker_file))
                self.ticker.start()
                threas.append(self.ticker)
                print(f'Запущен поток - {threading.activeCount()}')

        for flow in threas:
            flow.join()
            print(f'Остановлен поток - {threading.activeCount()}')

            if flow.volatility == 0:
                self.zero_volatility.append(flow.name_ticker)

            else:
                self.ticker_dictionary[flow.name_ticker] = flow.volatility


def main():
    counter = 0
    path = str(Path("trades"))
    data = Data(path)
    data.data_output()

    data.ticker_dictionary = sorted(data.ticker_dictionary.items(), key=operator.itemgetter(1))
    data.ticker_dictionary.reverse()

    for ticker_list in data.ticker_dictionary[:3], data.ticker_dictionary[-3:]:
        if counter < len(data.ticker_dictionary[:3]):
            print('Максимальная волатильность:')
        if counter >= len(data.ticker_dictionary[:3]):
            print('Минимальная волатильность:')
        for ticker_volatility in ticker_list:
            print(f' {ticker_volatility[0]} - {ticker_volatility[1]} %', flush=True)
            counter += 1
    print('Нулевая волотильность:')
    list_ticker_name = [ticker_name for ticker_name in data.zero_volatility]
    print(','.join(map(str, list_ticker_name)), flush=True)


if __name__ == "__main__":
    main()

# Зачёт!
