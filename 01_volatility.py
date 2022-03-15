# -*- coding: utf-8 -*-
import pandas as pd
import os
from pathlib import Path
import operator


# Описание предметной области:
#
# При торгах на бирже совершаются сделки - один купил, второй продал.
# Покупают и продают ценные бумаги (акции, облигации, фьючерсы, етс). Ценные бумаги - это по сути долговые расписки.
# Ценные бумаги выпускаются партиями, от десятка до несколько миллионов штук.
# Каждая такая партия (выпуск) имеет свой торговый код на бирже - тикер - https://goo.gl/MJQ5Lq
# Все бумаги из этой партии (выпуска) одинаковы в цене, поэтому говорят о цене одной бумаги.
# У разных выпусков бумаг - разные цены, которые могут отличаться в сотни и тысячи раз.
# Каждая биржевая сделка характеризуется:
#   тикер ценнной бумаги
#   время сделки
#   цена сделки
#   обьем сделки (сколько ценных бумаг было куплено)
#
# В ходе торгов цены сделок могут со временем расти и понижаться. Величина изменения цен называтея волатильностью.
# Например, если бумага №1 торговалась с ценами 11, 11, 12, 11, 12, 11, 11, 11 - то она мало волатильна.
# А если у бумаги №2 цены сделок были: 20, 15, 23, 56, 100, 50, 3, 10 - то такая бумага имеет большую волатильность.
# Волатильность можно считать разными способами, мы будем считать сильно упрощенным способом -
# отклонение в процентах от полусуммы крайних значений цены за торговую сессию:
#   полусумма = (максимальная цена + минимальная цена) / 2
#   волатильность = ((максимальная цена - минимальная цена) / полусумма) * 100%
# Например для бумаги №1:
#   half_sum = (12 + 11) / 2 = 11.5
#   volatility = ((12 - 11) / half_sum) * 100 = 8.7%
# Для бумаги №2:
#   half_sum = (100 + 3) / 2 = 51.5
#   volatility = ((100 - 3) / half_sum) * 100 = 188.34%
#
# В реальности волатильность рассчитывается так: https://goo.gl/VJNmmY
#
# Задача: вычислить 3 тикера с максимальной и 3 тикера с минимальной волатильностью.
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
# Подготовка исходных данных
# 1. Скачать файл https://drive.google.com/file/d/1l5sia-9c-t91iIPiGyBc1s9mQ8RgTNqb/view?usp=sharing
#       (обратите внимание на значок скачивания в правом верхнем углу,
#       см https://drive.google.com/file/d/1M6mW1jI2RdZhdSCEmlbFi5eoAXOR3u6G/view?usp=sharing)
# 2. Раззиповать средствами операционной системы содержимое архива
#       в папку python_base/lesson_012/trades
# 3. В каждом файле в папке trades содержится данные по сделакам по одному тикеру, разделенные запятыми.
#   Первая строка - название колонок:
#       SECID - тикер
#       TRADETIME - время сделки
#       PRICE - цена сделки
#       QUANTITY - количество бумаг в этой сделке
#   Все последующие строки в файле - данные о сделках
#
# Подсказка: нужно последовательно открывать каждый файл, вычитывать данные, высчитывать волатильность и запоминать.
# Вывод на консоль можно сделать только после обработки всех файлов.
#
# Для плавного перехода к мультипоточности, код оформить в обьектном стиле, используя следующий каркас
#
# class <Название класса>:
#
#     def __init__(self, <параметры>):
#         <сохранение параметров>
#
#     def run(self):
#         <обработка данных>

class Ticker:
    volatility_ticker = None

    def __init__(self, full_path):
        self.full_path = full_path

    def run(self):
        try:
            self.volatility_calculation()
        except Exception as exc:
            print(exc)

    def volatility_calculation(self):
        frame_price = pd.read_csv(self.full_path)
        max_price = frame_price['PRICE'].max()
        min_price = frame_price['PRICE'].min()
        half_sum = (max_price + min_price) / 2
        # self.volatility = round(((max_price - min_price) / half_sum) * 100, 2)
        # может вообще тогда не сохранять в переменную? А сразу вернуть результат?
        #  Здесь это не имеет значения, но в многопоточном варианте получить результат
        #  вычислений из переменной класса будет проще всего.
        return round(((max_price - min_price) / half_sum) * 100, 2)


class Data:
    volatility_dict = {}  # словарь(ключ-имя бумаги,значение-волотильность бумаги
    zero_volatility = []  # бумаги с нулевой волотильностью
    ticker_dictionary = {}

    def __init__(self, my_path):
        self.my_path = my_path
        self.ticker = None

    def data_output(self):
        for self.address, self.dirs, self.files in os.walk(self.my_path):
            for self.ticker_file in self.files:
                self.ticker = Ticker(os.path.join(self.address, self.ticker_file))
                self.volatility_dict[self.ticker_file] = self.ticker.volatility_calculation()

        for self.key, self.value in self.volatility_dict.items():
            if self.value == 0:
                self.zero_volatility.append(self.key[7:11])
            else:
                self.ticker_dictionary[self.key[7:11]] = self.value


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
            print(f' {ticker_volatility[0]} - {ticker_volatility[1]} %')
            counter += 1
    print('Нулевая волотильность:')
    list_ticker_name = [ticker_name for ticker_name in data.zero_volatility]
    print(','.join(map(str, list_ticker_name)))


if __name__ == "__main__":
    main()

# Зачёт!
