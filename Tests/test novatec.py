import pandas
import openpyxl
from mailRu_test import MailClient
from datetime import datetime
import os
import requests as r
import requests.exceptions
import time
import json


cards = {}
transactions = []
dates = []


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
                      'deviation': 180,
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
                        print(veh_number)
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


def get_cards():
    excel_data_df = pandas.read_excel('ATL.xlsx')
    all_cards = excel_data_df['НОВАТЭК'].tolist()
    for card in all_cards:
        if pandas.isnull(card):
            continue
        else:
            index = all_cards.index(card)
            cards[str(card)] = excel_data_df['VIN'].tolist()[index]


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


def get_transactions():
    min_max_dates = []
    mail_ru = MailClient(login='report2@a-lain.ru', password='eHLYRrFsMPnsHz4ggu1d', mail='mail.ru')
    unseen_mails = mail_ru.search_unseen_mails_in_folder('ATL_Novatec')
    if len(unseen_mails) == 0:
        print('Новых писем с отчетами не найдено!')
    else:
        print(f'Найдено новых писем: {len(unseen_mails)}')
    for mail in unseen_mails:
        files = mail_ru.download_mail_attach(mail, '.xlsx')
        for file in files:
            wb = openpyxl.load_workbook(file)
            sheet = wb.active
            cardNums = [row[1].value for row in sheet.rows][1:]
            dates = [row[3].value for row in sheet.rows][1:]
            times = [row[4].value for row in sheet.rows][1:]
            posBrands = [row[5].value for row in sheet.rows][1:]
            serviceNames = [row[6].value for row in sheet.rows][1:]
            amounts = [row[7].value for row in sheet.rows][1:]
            prices = [row[8].value for row in sheet.rows][1:]
            sums = [row[9].value for row in sheet.rows][1:]
            cardNum = 0
            for i in range(len(cardNums)):
                if dates[i] is None or amounts[i] is None:
                    continue
                if cardNums[i] is not None:
                    cardNum = cardNums[i][3:-1]
                date_time = '-'.join(reversed(str(dates[i]).split('-'))) + 'T' + times[i]
                min_max_dates.append('-'.join(reversed(str(dates[i]).split('-'))))
                transactions.append({'cardNum': cardNum,
                                     'date': date_time,
                                     'amount': amounts[i],
                                     'price': prices[i],
                                     'sum': sums[i],
                                     'posBrand': posBrands[i],
                                     'posTown': '',
                                     'latitude': 0,
                                     'longitude': 0,
                                     'posAddress': '',
                                     'serviceName': serviceNames[i]
                                     })
            os.remove(file)
    # print(min(min_max_dates))
    # print(max(min_max_dates))
    mail_ru.logout()


# get_cards()
get_transactions()
print(transactions)
# wialon = WialonClient(0, 0)
# wialon.login()
# wialon.reg_card(cards=cards, name='Новатэк')
# wialon.logout()