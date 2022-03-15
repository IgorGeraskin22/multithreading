# -*- coding: utf-8 -*-
import csv
import operator
import os
import time
from multiprocessing import Process, Queue
from queue import Empty
from concurrent.futures import ProcessPoolExecutor


# Задача: вычислить 3 тикера с максимальной и 3 тикера с минимальной волатильностью в МНОГОПРОЦЕССНОМ стиле
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
class PathCreator:

    def __init__(self, path):
        self.path = path
        self.file_paths = []

    def run(self):
        for dir_path, dir_names, files in os.walk(self.path):
            for file in files:
                self.file_paths.append(os.path.normpath(os.path.join(dir_path, file)))


class VolatilityCounter(Process):
    def __init__(self, file_path, queue):
        super().__init__()
        self.file_path = file_path
        self.queue = queue
        self.volatility = None
        self.price_min = None
        self.price_max = None
        self.ticker = None

    def _volatility_calculation(self):
        half_sum = (self.price_max + self.price_min) / 2
        self.volatility = round((self.price_max - self.price_min) * 100 / half_sum, 3)

    def run(self):
        with open(self.file_path, mode='r', encoding='cp1251') as opened_file:
            self.price_min = self.price_max = None
            for line_num, line in enumerate(csv.reader(opened_file, delimiter=",")):
                if line_num != 0:
                    price = float(line[2])
                    if not self.price_max or price > self.price_max:
                        self.price_max = price
                    if not self.price_min or price < self.price_min:
                        self.price_min = price

            self.ticker = line[0]  # Взял имя бумаги
            self._volatility_calculation()
            self.queue.put([self.ticker, self.volatility])  # Добавил в очередь имя бумаги и волотильность


if __name__ == '__main__':
    start = time.time()
    source_path = 'trades'
    res = []
    zero_vol = []
    source_path = os.path.normpath(source_path)
    filer = PathCreator(path=source_path)
    filer.run()

    queue = Queue(maxsize=3)
    brokers = [VolatilityCounter(file_path=file_path, queue=queue) for file_path in filer.file_paths]

    for broker in brokers:
        broker.start()

    while True:
        try:
            ticker, vol = queue.get(block=True, timeout=1)
            res.append([ticker, vol]) if vol > 0 else zero_vol.append(ticker)
        except Empty:
            if not any(broker.is_alive() for broker in brokers):
                break

    for broker in brokers:
        broker.join()

    i = 0
    print('Максимальная волатильность:')
    for ticker, vol in sorted(res, key=operator.itemgetter(1), reverse=True):
        if i in range(3):
            print(ticker, vol)

        if i == len(res) - 3:
            print('Минимальная волатильность:')
        if i in range(len(res) - 3, len(res)):
            print(ticker, vol)

        i += 1

    print(f'Нулевая волатильность: \n {sorted(zero_vol)}')
    finish = time.time()

    print("Время параллельного выполнения:", int(finish - start))

# Зачёт!
