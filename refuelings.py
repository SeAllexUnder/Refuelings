import json
import time
import requests as r
from datetime import datetime
from base64 import b64encode
import requests.exceptions
import pandas
import os
import get_logo



class FuelCards_Client:
    '''
    token - токен, не обязателен
    baseURL - базовый URL для отправки запросов, ОБЯЗАТЕЛЕН
    cards - словарь карт водителя, ОБЯЗАТЕЛЕН. Ключи - номер карты, значение - название карты ({'12345': 'Камаз Т123ТТ35'}
    transactions - список всех транзакций за период, ОБЯЗАТЕЛЕН.
    Транзакция представляет собой словарь. Необходимо, чтобы были созданы следующие ключи словаря:
        'date' - дата и время транзакции
        'cardNum' - номер карты водителя
        'amount' - объем транзакции
        'price' - цена топлива
        'sum' - сумма транзакции
        'posBrand' и 'posTown' - отображают инфо об АЗС. Можно оставить пустыми
        'latitude' - широта АЗС
        'longitude' - - долгота АЗС
        'posAddress' - адрес АЗС
        'serviceName' - тип топлива

    При создании новых типов личных кабинетов:
    Создать класс, наследующийся от FuelCards_Client
    Прописать ОБЯЗАТЕЛЬНЫЕ функции класса:
        __init__ - по умолчанию при создании объекта необходимо указать token и baseURL.
            Если API работает по паре логин-пароль - переопределить функцию.
        get_responce - функция для отправки запроса и обработки ошибок. Скопировать ее из другого,
            уже существующего класса, и переделать формат запроса
        get_cards - функция, возвращающая переменную cards класса (self.cards). Формат переменной описан выше
        get_transactions - функция, возвращающая transactions класса (self.transactions). Формат переменной описан выше
        get_region_timezone - в случае необходимости корректировки времени по временным зонам, не переназначать.
            Если корректировка не требуется - прописать return unix_date
    После создания и тестирования класса, в функции main прописать условие:
        elif cabinet["name"] == "Название л.к."
        В нем создать объект соответствующего класса.
    '''
    token = ''
    baseURL = ''
    cards = {}
    transactions = []
    timezones = {-1: ["Калининградская область"],
                 0: ["Архангельская область",
                     "Белгородская область",
                     "Брянская область",
                     "Владимирская область",
                     "Волгоградская область",
                     "Вологодская область",
                     "Воронежская область",
                     "Ивановская область",
                     "Калужская область",
                     "Кировская область",
                     "Костромская область",
                     "Курская область",
                     "Ленинградская область",
                     "Липецкая область",
                     "Московская область",
                     "Мурманская область",
                     "Нижегородская область",
                     "Новгородская область",
                     "Орловская область",
                     "Пензенская область",
                     "Псковская область",
                     "Ростовская область",
                     "Рязанская область",
                     "Смоленская область",
                     "Тамбовская область",
                     "Тверская область",
                     "Тульская область",
                     "Ярославская область",
                     "Краснодарский край",
                     "Ставропольский край",
                     "Республика Адыгея (Адыгея)",
                     "Республика Дагестан",
                     "Республика Ингушетия",
                     "Кабардино-Балкарская Республика",
                     "Республика Калмыкия",
                     "Карачаево-Черкесская Республика",
                     "Республика Карелия",
                     "Республика Коми",
                     "Республика Крым",
                     "Республика Марий Эл",
                     "Республика Мордовия",
                     "Мордовия, республика",
                     "Республика Северная Осетия - Алания",
                     "Республика Татарстан (Татарстан)",
                     "Республика Татарстан",
                     "Татарстан, республика",
                     "Чеченская Республика",
                     "Чувашская Республика - Чувашия",
                     "Чувашская республика",
                     "Ненецкий автономный округ",
                     "Юго-Западный административный округ",
                     "Южный административный округ",
                     "Северо-Западный административный округ",
                     "Чувашская Республика",
                     "Москва",
                     "Новомосковский административный округ",
                     "Троицкий административный округ",
                     "Колпинский район",
                     "Пушкинский район",
                     "Республика Адыгея",
                     "Выборгский район",
                     "Санкт-Петербург",
                     "Санкт-петербург",
                     "Западный административный округ",
                     "Восточный административный округ",
                     "Марий ел, республика",
                     "Фрунзенский район",
                     "Северо-Восточный административный округ",
                     "Невский район",
                     "Московский район"],
                 1: ["Астраханская область",
                     "Самарская область",
                     "Саратовская область",
                     "Ульяновская область",
                     "Удмуртская Республика",
                     "Удмуртская республика"],
                 2: ["Курганская область",
                     "Оренбургская область",
                     "Свердловская область",
                     "Тюменская область",
                     "Челябинская область",
                     "Пермский край",
                     "Республика Башкортостан",
                     "Башкортостан, республика",
                     "Ханты-Мансийский автономный округ",
                     "Ямало-Ненецкий автономный округ"],
                 3: ["Омская область"],
                 4: ["Кемеровская область",
                     "Новосибирская область",
                     "Томская область",
                     "Алтайский край",
                     "Красноярский край",
                     "Республика Алтай",
                     "Республика Тыва",
                     "Республика Хакасия"],
                 5: ["Иркутская область",
                     "Республика Бурятия"],
                 6: ["Республика Саха",
                     "Амурская область",
                     "Забайкальский край"],
                 7: ["Приморский край",
                     "Хабаровский край",
                     "Еврейская автономная область"],
                 8: ["Магаданская область",
                     "Сахалинская область"],
                 9: ["Камчатский край",
                     "Чукотский автономный округ"]
                 }

    def __init__(self, token='', baseURL=''):
        self.token = token
        self.baseURL = baseURL

    def get_region_timezone(self, unix_date, adress):
        correct_unix_date = 0
        correct_adress = adress.split(', ')[1]
        for timezone in self.timezones.keys():
            if correct_adress in self.timezones[timezone]:
                correct_unix_date = unix_date - (timezone*3600)
                break
        if correct_unix_date == 0:
            print(f'Адрес {adress} не найден!')
            correct_unix_date = unix_date
        return correct_unix_date


class FuelCards_Client_PPR(FuelCards_Client):

    def get_responce(self, URL, headers):
        while True:
            try:
                responce = r.get(url=URL, headers=headers)
                if responce.status_code == 200:
                    return responce.json()
                else:
                    print(f'{self.baseURL} - ошибка подключения! Попытка повторного запроса через 60 сек...')
                    time.sleep(60)
            except requests.exceptions.ConnectTimeout:
                print(f'{self.baseURL} - превышено время ожидания ответа! Попытка повторного запроса через 60 сек...')
                time.sleep(60)

    def get_cards(self):
        """
        :return: Информация по картам
        """
        URL = f'{self.baseURL}/api/public-api/v2/cards'
        headres = {'accept': 'application/json',
                   'Authorization': self.token}
        response = self.get_responce(URL, headres)
        try:
            for card in response['cards']:
                URL = f'{self.baseURL}/api/public-api/v2/cards/{card["cardNum"]}/bindings'
                headres = {'accept': 'application/json',
                           'Authorization': self.token}
                resp = self.get_responce(URL, headres)
                self.cards[card['cardNum']] = resp['emp']
        except KeyError:
            print('Ошибка подключения к л.к. топливных карт!')
        return self.cards

    def get_transactions(self, dateFrom, dateTo):
        """
        :return: Информация по транзакциям за период
        """
        URL = f'{self.baseURL}/api/public-api/v2/transactions?dateFrom={dateFrom}&dateTo={dateTo}&format=json'
        headres = {'accept': 'application/json',
                   'Authorization': self.token}
        response = self.get_responce(URL, headres)
        try:
            self.transactions = response['transactions']
        except KeyError:
            print('Ошибка подключения к л.к. топливных карт!')
        return self.transactions

    def get_region_timezone(self, unix_date, adress):
        return unix_date


class FuelCards_Client_Rosneft(FuelCards_Client):
    login = ''
    pwd = ''
    contract_code = ''

    def __init__(self, login='', password='', contract_code='', baseURL=''):
        self.login = login
        self.pwd = self.encode_base64(password)
        self.contract_code = contract_code
        self.baseURL = baseURL

    def get_responce(self, URL, headers, params):
        while True:
            try:
                responce = r.get(url=URL, headers=headers, params=params)
                if responce.status_code == 200:
                    return responce.json()
                else:
                    print(f'{self.baseURL} - ошибка подключения! Попытка повторного запроса через 60 сек...')
                    time.sleep(60)
            except requests.exceptions.ConnectTimeout:
                print(f'{self.baseURL} - превышено время ожидания ответа! Попытка повторного запроса через 60 сек...')
                time.sleep(60)

    def encode_base64(self, string):
        string_bytes = string.encode('ascii')
        base64_bytes = b64encode(string_bytes)
        base64_string = base64_bytes.decode('ascii')
        return base64_string

    def get_cards(self):
        """
        :return: Информация по топливным картам
        """
        URL = f'{self.baseURL}/api/emv/v1/GetCardsByContract'
        headers = {'RnCard-Identity-Account-Pass': self.pwd}
        params = {'u': self.login,
                  'p': '',
                  'contract': self.contract_code,
                  'type': 'json'}
        responce = self.get_responce(URL, headers, params)
        for card in responce:
            name = card['Rem']
            try:
                name_split = card['Rem'].split(':')[1].split(' ')[1]
                if len(name_split) >= 6:
                    name = name_split
            except IndexError:
                pass
            # print(name)
            self.cards[card['Num']] = name
        return self.cards

    def get_transactions(self, dateFrom, dateTo):
        URL = f'{self.baseURL}/api/emv/v1/GetOperByContract'
        headers = {'RnCard-Identity-Account-Pass': self.pwd}
        params = {'contract': self.contract_code,
                  'u': self.login,
                  'p': '',
                  'begin': f'{dateFrom}T00:00:00',
                  'end': f'{dateTo}T23:59:59',
                  'type': 'json'}
        responce = self.get_responce(URL, headers, params)
        try:
            if responce['OperationList'] is not None:
                for transaction in responce['OperationList']:
                    if transaction['GCat'] == 'FUEL':
                        value = float(transaction["Value"])
                        if transaction["Type"] == 24:
                            value = float(transaction["Value"])*(-1)
                        # print(f'{transaction["Card"]} {transaction["Type"]} {value}')
                        self.transactions.append({'cardNum': transaction['Card'],
                                             'date': transaction['Date'],
                                             'amount': value,
                                             'price': transaction['Price'],
                                             'sum': transaction['Sum'],
                                             'posBrand': transaction['Address'],
                                             'posTown': '',
                                             'latitude': 0,
                                             'longitude': 0,
                                             'posAddress': transaction['Address'],
                                             'serviceName': transaction['GName'],
                                             'Code': transaction['Code'],
                                             'Ref': transaction['Ref'],
                                             'Type': transaction['Type']
                                             })
        except KeyError:
            print(f'Key Error. Responce: {responce}')
            # time.sleep(60)
            # self.get_transactions(dateFrom, dateTo)
        # print(self.transactions)
        return self.transactions


class FuelCards_Client_Tatneft(FuelCards_Client):

    def __init__(self, login, password, baseURL, dateFrom, dateTo):
        self.baseURL = baseURL
        self.token = self.get_token(login, password)
        URL = f'{self.baseURL}action/reportJob/tnp.mainData/$all'
        headers = {'content-type': 'application/json',
                   'accept': 'application/json',
                   'authorization': f'Bearer {self.token}'}
        data = {'dateFrom': dateFrom,
                'dateTo': dateTo,
                'format': "XLSX",
                'reportParams': ';P_DATE_FROM=Y;P_DATE_TO=Y;P_FMT=;P_ISS_CONTRACT=Y;',
                'reportTemplate': '22020702'}
        print('Запрашиваю отчет')
        self.get_response(URL=URL, data=data, headers=headers)
        time.sleep(60)
        # report_id = 8582401
        report_id = self.search_report(dateTo)
        self.get_report(report_id)
        self.set_cards(report_id)
        self.set_transactions(report_id)
        os.remove(f'transactions {report_id}.xlsx')

    def get_token(self, login, pwd):
        URL = f'{self.baseURL}authenticate'
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}
        data = {'domain': 'DEFAULT',
                'password': pwd,
                'username': login}
        responce = self.get_response(URL=URL, data=data, headers=headers)
        id_token = responce['id_token']
        return id_token

    def get_response(self, URL, data, headers):
        while True:
            try:
                responce = r.post(url=URL, data=json.dumps(data), headers=headers)
                if responce.status_code == 200:
                    return responce.json()
                else:
                    print(f'{URL} - ошибка подключения! Попытка повторного запроса через 60 сек...')
                    time.sleep(60)
            except requests.exceptions.ConnectTimeout:
                print(f'{URL} - превышено время ожидания ответа! Попытка повторного запроса через 60 сек...')
                time.sleep(60)

    def search_report(self, dateFrom):
        URL = f'{self.baseURL}data/reportList/tnp.reportData/$all'
        headers = {'content-type': 'application/json',
                   'accept': 'application/json',
                   'authorization': f'Bearer {self.token}'}
        data = {'agreementNumber': 'З050005591',
                'dateFrom': dateFrom,
                'pager': {'page': 1, 'pageSize': 20}}
        responce = self.get_response(URL=URL, data=data, headers=headers)
        for report in responce['data']:
            if report['id'] != 0 and report['id'] is not None:
                report_id = report['id']
                return report_id

    def get_report(self, id_report):
        print('Получаю отчет')
        URL = f'{self.baseURL}report/{id_report}/Transactions_{id_report}.xlsx'
        headers = {'content-type': 'application/json',
                   'accept': 'application/json',
                   'authorization': f'Bearer {self.token}'}
        while True:
            try:
                responce = r.get(url=URL, headers=headers, allow_redirects=True)
                if responce.status_code == 200:
                    open(f'transactions {id_report}.xlsx', 'wb').write(responce.content)
                    break
                else:
                    print(f'{URL} - ошибка подключения! Попытка повторного запроса через 60 сек...')
                    time.sleep(60)
            except requests.exceptions.ConnectTimeout:
                print(f'{URL} - превышено время ожидания ответа! Попытка повторного запроса через 60 сек...')
                time.sleep(60)

    def set_cards(self, id_report):
        excel_data_df = pandas.read_excel(f'transactions {id_report}.xlsx',
                                          sheet_name='TN_B_PeriodContractTransactions')
        for card in excel_data_df['Unnamed: 1'].tolist()[1:]:
            index = excel_data_df['Unnamed: 1'].tolist()[1:].index(card)
            driver = excel_data_df['Unnamed: 2'].tolist()[1:][index]
            if pandas.isnull(driver):
                self.cards[card] = 'Не назначен'
            else:
                try:
                    self.cards[card] = driver.split(': ')[1].split(' ')[0]
                except IndexError:
                    self.cards[card] = driver.split(': ')[1]

    def set_transactions(self, id_report):
        excel_data_df = pandas.read_excel(f'transactions {id_report}.xlsx',
                                          sheet_name='TN_B_PeriodContractTransactions')
        for index in range(len(excel_data_df['Unnamed: 1'].tolist()[1:])):
            value = excel_data_df['Unnamed: 11'].tolist()[1:][index]
            if excel_data_df['Unnamed: 20'].tolist()[1:][index] == 'возврат':
                value = excel_data_df['Unnamed: 11'].tolist()[1:][index] * (-1)
            # print(value)
            date = str(excel_data_df['Unnamed: 0'].tolist()[1:][index])
            self.transactions.append({'cardNum': excel_data_df['Unnamed: 1'].tolist()[1:][index],
                                 'date': int(datetime.strptime(date, '%Y-%m-%d %H:%M:%S').timestamp()),
                                 'amount': value,
                                 'price': excel_data_df['Unnamed: 15'].tolist()[1:][index],
                                 'sum': excel_data_df['Unnamed: 16'].tolist()[1:][index],
                                 'posBrand': excel_data_df['Unnamed: 6'].tolist()[1:][index],
                                 'posTown': '',
                                 'latitude': 0,
                                 'longitude': 0,
                                 'posAddress': excel_data_df['Unnamed: 7'].tolist()[1:][index],
                                 'serviceName': excel_data_df['Unnamed: 10'].tolist()[1:][index],
                                 'Type': excel_data_df['Unnamed: 20'].tolist()[1:][index]
                                 })

    def get_cards(self):
        return self.cards

    def get_transactions(self, dateFrom, dateTo):
        return self.transactions

    def get_region_timezone(self, unix_date, adress):
        correct_unix_date = 0
        for timezone in self.timezones.keys():
            if adress in self.timezones[timezone]:
                correct_unix_date = unix_date - (timezone*3600)
                break
        if correct_unix_date == 0:
            print(f'Адрес {adress} не найден!')
            correct_unix_date = unix_date
        return correct_unix_date


class FuelCards_Client_Gazprom(FuelCards_Client):
    contract_code = ''

    def __init__(self, token, contract_code='',baseURL=''):
        self.token = token
        self.contract_code = contract_code
        self.baseURL = baseURL

    def get_responce(self, URL, headers, params):
        while True:
            try:
                responce = r.post(url=URL, headers=headers, data=json.dumps(params))
                if responce.status_code == 200:
                    return responce.json()
                else:
                    print(f'{self.baseURL} - ошибка подключения! Попытка повторного запроса через 60 сек...')
                    time.sleep(60)
            except requests.exceptions.ConnectTimeout:
                print(f'{self.baseURL} - превышено время ожидания ответа! Попытка повторного запроса через 60 сек...')
                time.sleep(60)

    def get_cards(self):
        URL = f'{self.baseURL}/api/api_v2.php'
        params = {'action': 'cards',
                  'key': '{key}',
                  'cont': self.contract_code}
        headers = {'apikey': self.token,
                   'Content-Type': 'application/json'}
        responce = self.get_responce(URL=URL, headers=headers, params=params)
        for card in responce['result']:
            if card['stat'] == 'Активна':
                self.cards[card['lognb']] = card['user'].split(' ')[0]
        return self.cards

    def get_transactions(self, date_From, date_To):
        URL = f'{self.baseURL}/api/api_v2.php'
        params = {'action': 'opers',
                  'key': '{key}',
                  'cont': self.contract_code,
                  'dtemin': date_From,
                  'dtemax': date_To}
        headers = {'apikey': self.token,
                   'Content-Type': 'application/json'}
        responce = self.get_responce(URL=URL, headers=headers, params=params)
        for transaction in responce['result']:
            if transaction['vidgsm'] != 'tovar':
                date_time = transaction['dte'].split(' ')[0] + 'T' + transaction['dte'].split(' ')[1]
                self.transactions.append({'cardNum': transaction['lognb'],
                                          'date': date_time,
                                          'amount': transaction['kolich'],
                                          'price': transaction['zenaclients'],
                                          'sum': transaction['skidkaclients'],
                                          'posBrand': transaction['numazs'],
                                          'posTown': '',
                                          'latitude': 0,
                                          'longitude': 0,
                                          'posAddress': transaction['torgovtochka'],
                                          'serviceName': transaction['tovar'],
                                          })
        return self.transactions

    def get_region_timezone(self, unix_date, adress):
        return unix_date


class WialonClient:
    URL = 'https://hst-api.wialon.com'
    TOKEN = '2591a1ab8e3fe83813c057daf0d62d2b78BB480C5EFF6B9B9986F83900CC20E49E5E2772'
    EID = 0
    USER_ID = ''
    dateFrom = ''
    dateTo = ''

    def __init__(self, dateFrom, dateTo):
        self.dateFrom = dateFrom
        self.dateTo = dateTo

    def get_responce(self, URL):
        while True:
            try:
                responce = r.post(url=URL)
                if responce.status_code == 200:
                    return responce.json()
                else:
                    print(f'{self.URL} - ошибка подключения! Попытка повторного запроса через 60 сек...')
                    time.sleep(60)
            except requests.exceptions.ConnectTimeout:
                print(f'{self.URL} - превышено время ожидания ответа! Попытка повторного запроса через 60 сек...')
                time.sleep(60)

    def login(self):
        '''Логин в систему виалон, получение eid'''
        login_completed = False
        params = {"token": self.TOKEN}
        svc = 'token/login'
        URL = f'{self.URL}/wialon/ajax.html?svc={svc}&params={json.dumps(params)}'
        responce = self.get_responce(URL)
        if 'error' in responce:
            print('Ошибка подключения к Виалону!')
        else:
            self.EID = responce['eid']
            self.USER_ID = responce['user']['id']
        if self.EID != 0:
            login_completed = True
        return login_completed

    def logout(self):
        '''Выход из системы Виалон'''
        logout_completed = False
        params = {}
        svc = 'core/logout'
        URL = f'{self.URL}/wialon/ajax.html?svc={svc}&params={json.dumps(params)}&sid={self.EID}'
        responce = self.get_responce(URL)
        if responce['error'] == 0:
            logout_completed = True
        return logout_completed

    def event_registration(self, info, register):
        # print(register)
        # print(info)
        """
        :param info: информация по транзакциям
        :return: None
        """
        pos = -1
        print(' ')
        # print(info)
        try:
            print(len(info['cardNum']))
        except KeyError:
            return None
        for card in info['cardNum']:
            pos += 1
            svc = 'unit/registry_fuel_filling_event'
            vehicle_id = self.vehicle_search(card)
            value_text = f'заправка: {info["amounts"][pos]} л; '
            # print(float(info["amounts"][pos]))
            if float(info["amounts"][pos]) < 0:
                print(f'Возврат: {info["amounts"][pos]} л')
                value_text = f'возврат: {info["amounts"][pos]} л; '
            else:
                print(f'Заправка: {info["amounts"][pos]} л')
            params = {'date': info['dates'][pos],
                      'volume': info['amounts'][pos],
                      'cost': info['prices'][pos],
                      'location': info['posAddress'][pos],
                      'deviation': 30,
                      'x': info['longitude'][pos],
                      'y': info['latitude'][pos],
                      'description': f'{datetime.utcfromtimestamp(info["dates"][pos] + 10800).strftime("%d.%m.%Y %H:%M")} - '
                                     f'{value_text}'
                                     f'цена: {info["prices"][pos]} р; '
                                     f'сумма: {info["sums"][pos]} р; '
                                     f'тип топлива: {info["serviceName"][pos]}; '
                                     f'АЗС: {info["posBrands"][pos]}; '
                                     f'Водитель: {info["drivers"][pos]}',
                      'itemId': vehicle_id
                      }
            URL = f'{self.URL}/wialon/ajax.html?svc={svc}&params={json.dumps(params)}&sid={self.EID}'
            registered_transactions = self.get_registered_transactions(vehicle_id)
            # print(registered_transactions)
            not_registered = True
            for date_transaction in registered_transactions.keys():
                # print(registered_transactions[date_transaction])
                if date_transaction == info['dates'][pos]:
                    value = registered_transactions[date_transaction][0]
                    # print(float(value))
                    # print(float(info["amounts"][pos]))
                    if float(value) == float(info["amounts"][pos]):
                        not_registered = False
                        print(
                            f'Событие '
                            f'{datetime.utcfromtimestamp(info["dates"][pos] + 10800).strftime("%d.%m.%Y %H:%M")} '
                            f'объемом {info["amounts"][pos]} л было зарегистрировано ранее!')
                    else:
                        self.delete_transaction(vehicle_id, info['dates'][pos])
                        print(f'Событие '
                              f'{datetime.utcfromtimestamp(info["dates"][pos] + 10800).strftime("%d.%m.%Y %H:%M")} '
                              f'перерегистрировано из-за разницы объемов.'
                              f'Было: {float(value)} - Стало: {float(info["amounts"][pos])}')
            if register and not_registered and vehicle_id is not None:
                self.get_responce(URL)
                print(f'Событие '
                      f'{datetime.utcfromtimestamp(info["dates"][pos] + 10800).strftime("%d.%m.%Y %H:%M")} '
                      f'объемом {info["amounts"][pos]} л зарегистрировано.')
            print(' ')

    def delete_transaction(self, vechicle_id, date_transaction):
        svc = 'messages/load_interval'
        params = {'itemId': vechicle_id,
                  'timeFrom': date_transaction,
                  'timeTo': date_transaction,
                  'flags': 1568,
                  'flagsMask': 1568,
                  'loadCount': 4294967295
                  }
        URL = f'{self.URL}/wialon/ajax.html?svc={svc}&params={json.dumps(params)}&sid={self.EID}'
        responce = self.get_responce(URL)
        # print(responce)
        if responce['count'] > 0:
            i = 0
            for message in responce['messages']:
                # print(message)
                svc = 'messages/delete_message'
                params = {"msgIndex": i,
                          'flags': 1536}
                URL = f'{self.URL}/wialon/ajax.html?svc={svc}&params={json.dumps(params)}&sid={self.EID}'
                responce = self.get_responce(URL)
                # print(responce)
                i += 1

    def get_registered_transactions(self, vechicle_id):
        """
        :param vechicle_id: ИД объекта
        :return: список дат зарегистрированных событий (UNIX-time)
        """
        svc = 'messages/load_interval'
        params = {'itemId': vechicle_id,
                  'timeFrom': int(datetime.strptime(self.dateFrom, '%Y-%m-%d').timestamp()) - 86400,
                  'timeTo': int(datetime.strptime(self.dateTo, '%Y-%m-%d').timestamp()) + 183599,
                  'flags': 1568,
                  'flagsMask': 1568,
                  'loadCount': 4294967295
                  }
        URL = f'{self.URL}/wialon/ajax.html?svc={svc}&params={json.dumps(params)}&sid={self.EID}'
        responce = self.get_responce(URL)
        registered_transactions = {}
        try:
            for message in responce['messages']:
                if message['t'] not in registered_transactions.keys():
                    registered_transactions[message['t']] = []
                registered_transactions[message['t']].append(message['p']['volume'])
        except KeyError:
            pass
        return registered_transactions

    def vehicle_search(self, card):
        """
        :param card: номер карты
        :return: айди объекта, на который зарегистрирована карта (прописана в произвольных полях)
        """
        params = {'spec': {'itemsType': 'avl_unit',
                           'propName': 'rel_customfield_value',
                           'propValueMask': card,
                           'sortType': 'sys_unique_id',
                           'propType': 'property',
                           },
                  'force': 1,
                  'flags': 4611686018427387903,
                  'from': 0,
                  'to': 0,
                  }
        svc = 'core/search_items'
        URL = f'{self.URL}/wialon/ajax.html?svc={svc}&params={json.dumps(params)}&sid={self.EID}'
        responce = self.get_responce(URL)
        if len(responce['items']) == 1:
            print(f"Операция по автомобилю {responce['items'][0]['nm']}")
            return responce['items'][0]['id']
        elif len(responce['items']) == 0:
            print(f'Топливная карта {card} не зарегистрирована ни на одном ТС!')
        else:
            print(f'Топливная карта {card} зарегистрирована на нескольких ТС!')

    def reg_card(self, cards, name):
        current_hour = datetime.utcfromtimestamp(int(time.time()) + 10800).strftime("%H")
        if current_hour == '09':
            for card in cards.keys():
                if cards[card] != '':
                    veh_number = cards[card]
                    # print(veh_number)
                    """
                    :param veh_number: номер ТС
                    :return: инфо об объекте
                    """
                    params = {'spec': {'itemsType': 'avl_unit',
                                       'propName': 'sys_name',
                                       'propValueMask': f'*{veh_number}*',
                                       'sortType': 'sys_unique_id',
                                       'propType': 'property',
                                       },
                              'force': 1,
                              'flags': 9,
                              'from': 0,
                              'to': 0,
                              }
                    svc = 'core/search_items'
                    URL = f'{self.URL}/wialon/ajax.html?svc={svc}&params={json.dumps(params)}&sid={self.EID}'
                    responce = self.get_responce(URL)
                    # print(responce)
                    if len(responce['items']) == 1:
                        veh_id = responce['items'][0]['id']
                        fields = {}
                        for item in responce['items'][0]['flds'].keys():
                            fields[responce['items'][0]['flds'][item]['v']] = responce['items'][0]['flds'][item]['id']
                        if card not in fields.keys():
                            self.append_field(veh_id=veh_id, card=card, name=name)
                            print(f'Карта {veh_number} - {card} зарегистрирована!')
                        for field in fields:
                            # print(field)
                            # print(fields)
                            field_id = fields[field]
                            for card in cards.keys():
                                if field == card and veh_number != cards[card]:
                                    print(f'{field} - ошибка занесения! Текущий г.н. - {veh_number}, правильный - {cards[card]}.')
                                    self.delete_field(veh_id=veh_id, field_id=field_id)
                                    print('Карта удалена.')
                    else:
                        print(f'{veh_number} не найден!')

    def append_field(self, veh_id, card, name):
        params = {'itemId': veh_id,
                  'id': 0,
                  'callMode': 'create',
                  'n': name,
                  'v': card}
        svc = 'item/update_custom_field'
        URL = f'{self.URL}/wialon/ajax.html?svc={svc}&params={json.dumps(params)}&sid={self.EID}'
        self.get_responce(URL)

    def delete_field(self, veh_id, field_id):
        params = {'itemId': veh_id,
                  'id': field_id,
                  'callMode': 'delete'}
        svc = 'item/update_custom_field'
        URL = f'{self.URL}/wialon/ajax.html?svc={svc}&params={json.dumps(params)}&sid={self.EID}'
        self.get_responce(URL)


def clear_date(date):
    """
    :param date: Такая себе дата
    :return: UNIX дата
    """
    unix_date_time = ''
    if date != '' and date is not None:
        try:
            date_time_parts = date.split('T')
            date_parts = date_time_parts[0].split('-')
            clean_date = f'{date_parts[2]}.{date_parts[1]}.{date_parts[0]}'
            time_parts = date_time_parts[1]
            clean_time = time_parts.split('.')[0]
            clean_date_time = f'{clean_date} {clean_time[:-3]}'
            unix_date_time = int(datetime.strptime(clean_date_time, '%d.%m.%Y %H:%M').timestamp())
        except (IndexError, AttributeError):
            unix_date_time = date
    return unix_date_time


def sort_dict(dictionary, key):
    """
    :param dictionary: словарь
    :param key: ключ, по которому нужна сортировка
    :return: отсортированный словарь
    """
    for_sorted = {}
    for i in range(len(dictionary[key])):
        for_sorted[dictionary[key][i]] = i
    for_sorted = dict(sorted(for_sorted.items()))
    new_dict = {}
    for pos in for_sorted.values():
        for key in dictionary.keys():
            if key not in new_dict:
                new_dict[key] = []
            new_dict[key].append(dictionary[key][pos])
    return new_dict


def main(dateFrom, dateTo):
    # Создание объектов для подключения к л.к. топливных карт и виалону по апи
    with open('Параметры API.json', encoding='utf-8') as p:
        parameters = json.load(p)
    for organisation in parameters.keys():
        # if organisation != 'АТЛ':
        #     continue
        print('------------------------------')
        print(f'Организация: {organisation}')
        for cabinet in parameters[organisation]:
            if cabinet['name'] != 'Газпром':
                continue
            date_From = dateFrom
            date_To = dateTo
            print(f'Личный кабинет {parameters[organisation].index(cabinet) + 1}: '
                  f'{cabinet["name"]}')
            if cabinet["name"] == "ППР":
                # continue
                fuel_cards_client = FuelCards_Client_PPR(token=cabinet['token'],
                                                         baseURL=cabinet['baseURL'])
            elif cabinet["name"] == "Роснефть":
                # continue
                fuel_cards_client = FuelCards_Client_Rosneft(login=cabinet['login'],
                                                             password=cabinet['password'],
                                                             contract_code=cabinet['contract code'],
                                                             baseURL=cabinet['baseURL'])
            elif cabinet["name"] == "Татнефть":
                current_day = datetime.utcfromtimestamp(int(time.time()) + 10800).strftime("%d")
                current_hour = datetime.utcfromtimestamp(int(time.time()) + 10800).strftime("%H")
                date_To = datetime.utcfromtimestamp(int(time.time()) + 10800).strftime("%Y-%m-%d")
                date_From = datetime.utcfromtimestamp(int(time.time()) + 10800 - 2851200).strftime("%Y-%m-%d")
                # date_From = '2022-11-01'
                # date_To = '2022-12-21'
                kostyl = False
                if current_day == '01' and current_hour == '09' or kostyl:
                    if kostyl:
                        date_From = '2022-12-01'
                    fuel_cards_client = FuelCards_Client_Tatneft(login=cabinet['login'],
                                                                 password=cabinet['password'],
                                                                 baseURL=cabinet['baseURL'],
                                                                 dateFrom=date_From,
                                                                 dateTo=date_To)
                else:
                    print(f'{cabinet["name"]} обновляется 1 числа каждого месяца')
                    continue
                # continue
            elif cabinet["name"] == "Газпром":
                fuel_cards_client = FuelCards_Client_Gazprom(token=cabinet['token'],
                                                             contract_code=cabinet['contract code'],
                                                             baseURL=cabinet['baseURL'])
            else:
                print(f'API {cabinet["name"]} пока не реализовано')
                continue
            print(f'Период с {date_From} по {date_To}')
            wialon = WialonClient(dateFrom=date_From, dateTo=date_To)
            # получение карт и транзакций
            try:
                cards = fuel_cards_client.get_cards()
                transactions = fuel_cards_client.get_transactions(date_From, date_To)
                print(f'Найдено транзакций за период : {len(transactions)}')
            except TimeoutError:
                print(f'Личный кабинет {cabinet["baseURL"]} - ошибка TimeoutError!')
                continue
            if len(transactions) != 0:
                # сбор информации
                all_info = {'cardNum': [],
                                   'drivers': [],
                                   'dates': [],
                                   'amounts': [],
                                   'prices': [],
                                   'sums': [],
                                   'posBrands': [],
                                   'latitude': [],
                                   'longitude': [],
                                   'posAddress': [],
                                   'serviceName': []
                                   }
                # print(transactions)
                for transaction in transactions:
                    transaction_date = fuel_cards_client.get_region_timezone(
                        unix_date=clear_date(transaction['date']), adress=transaction['posAddress'])
                    transaction['date'] = transaction_date
                    if transaction['cardNum'] != 0:
                        all_info['cardNum'].append(transaction['cardNum'])
                        all_info['drivers'].append(cards[transaction['cardNum']])
                        all_info['dates'].append(transaction['date'])
                        all_info['amounts'].append('%.2f' % transaction['amount'])
                        all_info['prices'].append('%.2f' % transaction['price'])
                        all_info['sums'].append('%.2f' % transaction['sum'])
                        all_info['posBrands'].append(f'{transaction["posBrand"]} {transaction["posTown"]}')
                        all_info['latitude'].append(transaction['latitude'])
                        all_info['longitude'].append(transaction['longitude'])
                        all_info['posAddress'].append(transaction['posAddress'])
                        all_info['serviceName'].append(transaction['serviceName'])
                # print(all_info)
                print(f'Транзакций по картам: {len(all_info["cardNum"])}')
                if wialon.login():
                    # print(all_info)
                    wialon.reg_card(cards, cabinet['name'])
                    wialon.event_registration(sort_dict(all_info, 'dates'), True)
                else:
                    print('Ошибка входа в Виалон!')
                wialon.logout()
                all_info.clear()
                # for_wialon_info.clear()
            cards.clear()
            transactions.clear()
            del fuel_cards_client
            del wialon
    current_time = datetime.utcfromtimestamp(int(time.time()) + 10800).strftime("%d.%m.%Y %H:%M:%S")
    print(f'{current_time} - готово.')
    print('===========================================================================================================')


current_time = datetime.utcfromtimestamp(int(time.time()) + 10800).strftime("%d.%m.%Y %H:%M:%S")
print(f'{current_time} старт программы')
while True:
    current_time = datetime.utcfromtimestamp(int(time.time()) + 10800).strftime("%M:%S")
    dateFrom = datetime.utcfromtimestamp(int(time.time()) + 10800 - 86400).strftime("%Y-%m-%d")
    dateTo = datetime.utcfromtimestamp(int(time.time()) + 10800).strftime("%Y-%m-%d")
    kostyl = False
    # or str(current_time) == '29:59'
    if str(current_time) == '59:59' or kostyl:
        print(
            f'{datetime.utcfromtimestamp(int(time.time()) + 10800).strftime("%d.%m.%Y %H:%M:%S")} - считываю данные...')
        if kostyl:
            dateFrom = '2022-12-01'
            dateTo = '2023-01-27'
        main(dateFrom, dateTo)
        if kostyl:
            break
    else:
        time.sleep(60)
