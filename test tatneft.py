import requests as r
import json
import time
import requests.exceptions
import urllib.request
import pandas
import os
import math
from datetime import datetime


def get_init(dateFrom, dateTo, login, pwd):
    id_token = get_token(login, pwd)
    URL = f'{baseURL}action/reportJob/tnp.mainData/$all'
    headers = {'content-type': 'application/json',
               'accept': 'application/json',
               'authorization': f'Bearer {id_token}'}
    data = {'dateFrom': dateFrom,
            'dateTo': dateTo,
            'format': "XLSX",
            'reportParams': ';P_DATE_FROM=Y;P_DATE_TO=Y;P_FMT=;P_ISS_CONTRACT=Y;',
            'reportTemplate': '22020702'}
    get_response(URL=URL, data=data, headers=headers)
    time.sleep(60)
    report_id = search_report(id_token, dateTo)
    get_report(id_token, report_id)


def get_response(URL, data, headers):
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


def get_token(login, pwd):
    URL = f'{baseURL}authenticate'
    headers = {'Content-Type': 'application/json',
               'Accept': 'application/json'}
    data = {'domain': 'DEFAULT',
            'password': pwd,
            'username': login}
    responce = get_response(URL=URL, data=data, headers=headers)
    # print(responce.json())
    id_token = responce['id_token']
    # print(responce['isAuthOver'])
    return id_token


def get_report(id_token, id_report):
    URL = f'{baseURL}report/{id_report}/Transactions_{id_report}.xlsx'
    headers = {'content-type': 'application/json',
               'accept': 'application/json',
               'authorization': f'Bearer {id_token}'}
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


def search_report(id_token, dateFrom):
    URL = f'{baseURL}data/reportList/tnp.reportData/$all'
    headers = {'content-type': 'application/json',
               'accept': 'application/json',
               'authorization': f'Bearer {id_token}'}
    data = {'agreementNumber': 'З050005591',
            'dateFrom': dateFrom,
            'pager': {'page': 1, 'pageSize': 20}}
    responce = get_response(URL=URL, data=data, headers=headers)
    report_id = responce['data'][0]['id']
    return report_id


def get_cards(id_report):
    cards = {}
    excel_data_df = pandas.read_excel(f'transactions {id_report}.xlsx', sheet_name='TN_B_PeriodContractTransactions')
    for card in excel_data_df['Unnamed: 1'].tolist()[1:]:
        index = excel_data_df['Unnamed: 1'].tolist()[1:].index(card)
        driver = excel_data_df['Unnamed: 2'].tolist()[1:][index]
        if pandas.isnull(driver):
            cards[card] = 'Не назначен'
        else:
            cards[card] = driver[14:]
    return cards


def get_transactions(id_report):
    excel_data_df = pandas.read_excel(f'transactions {id_report}.xlsx', sheet_name='TN_B_PeriodContractTransactions')
    transactions = []
    for index in range(len(excel_data_df['Unnamed: 1'].tolist()[1:])):
        value = excel_data_df['Unnamed: 11'].tolist()[1:][index]
        if excel_data_df['Unnamed: 20'].tolist()[1:][index] == 'возврат':
            value = excel_data_df['Unnamed: 11'].tolist()[1:][index]*(-1)
        date = str(excel_data_df['Unnamed: 0'].tolist()[1:][index])
        transactions.append({'cardNum': excel_data_df['Unnamed: 1'].tolist()[1:][index],
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
    return transactions


def get_back():
    pass


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
            except r.exceptions.ConnectTimeout:
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

    def vehicle_search2(self, veh_number, card):
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
        if len(responce['items']) == 1:
            fields = {}
            for field in responce['items'][0]['flds'].keys():
                fields[responce['items'][0]['flds'][field]['n']] = responce['items'][0]['flds'][field]['id']
            # for field in fields.keys():
            # if field == 'Роснефть':
            #     self.delete_field(responce['items'][0]['id'], fields[field])
            if 'Татнефть' not in fields.keys():
                self.append_field(responce['items'][0]['id'], card)
        else:
            print(f'{veh_number} не найден!')

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
        # print(responce['items'])
        for item in responce['items']:
            print(
                "_______________________________________________________________________________________________________")
            print(item['nm'])
        if len(responce['items']) == 1:
            print(f"Заправка автомобиля {responce['items'][0]['nm']}")
            return responce['items'][0]['id']
        elif len(responce['items']) == 0:
            print(f'Топливная карта {card} не зарегистрирована ни на одном ТС!')
        else:
            print(f'Топливная карта {card} зарегистрирована на нескольких ТС!')

    def vehicle_search_on_group(self, group_mask):
        params = {'spec': {'itemsType': 'avl_unit_group',
                           'propName': 'sys_name',
                           'propValueMask': f'*{group_mask}*',
                           'sortType': 'sys_name',
                           },
                  'force': 1,
                  'flags': 1,
                  'from': 0,
                  'to': 0,
                  }
        svc = 'core/search_items'
        URL = f'{self.URL}/wialon/ajax.html?svc={svc}&params={json.dumps(params)}&sid={self.EID}'
        responce = self.get_responce(URL)
        # print(responce)
        units = []
        for group in responce['items']:
            for unit in group['u']:
                if unit not in units:
                    units.append(unit)
            # print(group)
            # units += group['u']
        print(len(units))
        for vechicle_id in units:
            self.delete_transaction(vechicle_id)

    def append_field(self, veh_id, card):
        params = {'itemId': veh_id,
                  'id': 0,
                  'callMode': 'create',
                  'n': 'Татнефть',
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
        responce = self.get_responce(URL)
        print(responce)

    def delete_transaction(self, vechicle_id):
        svc = 'messages/load_interval'
        params = {'itemId': vechicle_id,
                  'timeFrom': self.dateFrom,
                  'timeTo': self.dateTo,
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
                print(message)
                svc = 'messages/delete_message'
                params = {"msgIndex": i,
                          'flags': 1536}
                URL = f'{self.URL}/wialon/ajax.html?svc={svc}&params={json.dumps(params)}&sid={self.EID}'
                responce = self.get_responce(URL)
                print(responce)
                i += 1


def get_cards_on_wialon():
    cards = {}
    filename = 'Статусы карт контракта_8498682.xlsx'
    excel_data_df = pandas.read_excel(filename, sheet_name='TN_A4_CardsInfo')
    fuel_cards = []
    fuel_cards_stat = []
    vehicle_numbers = []
    for i in range(len(excel_data_df['Unnamed: 0'].tolist())):
        if excel_data_df['Unnamed: 1'].tolist()[i] == 'Номер карты':
            fuel_cards.append(excel_data_df['Unnamed: 11'].tolist()[i])
        if excel_data_df['Unnamed: 11'].tolist()[i] == 'Статус':
            fuel_cards_stat.append(excel_data_df['Unnamed: 18'].tolist()[i])
        if excel_data_df['Unnamed: 1'].tolist()[i] == 'Агенты карты':
            if excel_data_df['Unnamed: 35'].tolist()[i+2] == 258 or excel_data_df['Unnamed: 35'].tolist()[i+4] == 258:
                pass
            else:
                if pandas.isnull(excel_data_df['Unnamed: 14'].tolist()[i+3]):
                    vehicle_numbers.append(excel_data_df['Unnamed: 8'].tolist()[i+3])
                else:
                    vehicle_numbers.append(excel_data_df['Unnamed: 14'].tolist()[i+3])
        if excel_data_df['Unnamed: 1'].tolist()[i] == 'Агенты карты отсутствуют':
            vehicle_numbers.append('Агенты карты отсутствуют')
    for i in range(len(fuel_cards_stat)):
        if fuel_cards_stat[i] == 'Card OK':
            cards[fuel_cards[i]] = vehicle_numbers[i]
    for key in cards.keys():
        if len(cards[key]) != 9:
            print(cards[key] + ' ' + key)
    # print(cards['7013420003015674'][:9])

    # cards = {'7826010118797972': 'Макаров Виталий РНД', '7826010118797980': 'Скляров Алексей РНД', '7826010118797998': '', '7826010118798004': '', '7826010118798012': '', '7826010118798020': '', '7826010118798038': '', '7826010118798046': '', '7826010118798053': '', '7826010118798061': '', '7826010118798079': ' О755АР198', '7826010118798087': '', '7826010118798095': '', '7826010118798103': '', '7826010118798111': '', '7826010118798129': '', '7826010118798137': '', '7826010118798145': ' А423ХВ716', '7826010118798152': '', '7826010118798160': '', '7826010118798178': '', '7826010118798186': '', '7826010118798194': '', '7826010118798202': '', '7826010118798210': ' О577ЕН198', '7826010118798228': ' О257НМ198', '7826010118798236': ' Р193УР799', '7826010118798244': ' Р044АР198', '7826010118798251': ' Е325КО797', '7826010118798269': ' В519РХ198', '7826010118798277': '', '7826010118798285': '', '7826010118798293': '', '7826010118798301': '', '7826010118798319': ' К610КН797', '7826010118798327': ' О411ВС198', '7826010118798335': '', '7826010118798343': '', '7826010118798350': ' Е802КК797', '7826010118798368': ' О436ХО198', '7826010118798376': ' О583ОТ198', '7826010118798384': ' В879КТ797', '7826010118798392': ' О810ВВ198', '7826010118798400': ' О586НВ198', '7826010118798418': ' О454ОА198', '7826010118798426': ' О810АУ198', '7826010118798434': ' Р674КК797', '7826010118798442': ' О434УС198', '7826010118798459': ' О436ТХ198', '7826010118798467': ' К600КН797', '7826010118798475': ' Р290АО198', '7826010118798483': ' Р748КО797', '7826010118798491': '', '7826010118798509': ' О581МС198 НОВЫЙ', '7826010118798517': ' О808СО198', '7826010118798525': ' О577КР198', '7826010118798533': ' Р282АО198', '7826010118798541': ' О403КН198', '7826010118798558': '', '7826010118798566': '', '7826010118798574': '', '7826010118798582': '', '7826010118798590': '', '7826010118798608': '', '7826010118798616': '', '7826010118798624': '', '7826010118798632': '', '7826010118798640': '', '7826010118798657': ' Р802АЕ797', '7826010118798665': ' О246АУ797', '7826010118798673': '', '7826010118798681': '', '7826010118798699': '', '7826010118798707': '', '7826010118798715': '', '7826010118798723': '', '7826010118798731': '', '7826010118798749': '', '7826010118798756': '', '7826010118798764': ' Х280КО198', '7826010118798772': ' М860АА77', '7826010118798780': '', '7826010118798798': '', '7826010118798806': ' О898хс198', '7826010118798814': ' М886АА797', '7826010118798822': ' Р662АО797', '7826010118798830': '', '7826010118798848': '', '7826010118798855': 'Венатовский  Александр', '7826010118798863': '', '7826010118795372': ' е966уе198', '7826010118795380': ' О874ХС198', '7826010118795398': ' О582МН198', '7826010118795406': ' О436ТН198', '7826010118795414': ' Е581НУ198', '7826010118795422': ' В796АС716', '7826010118795430': ' О436УВ198', '7826010118795448': ' В200ХЕ198', '7826010118795455': ' В826ТР198', '7826010118795463': ' О550ЕН198', '7826010118795471': ' Х507ОМ178', '7826010118795489': ' О813ХК198', '7826010118795497': ' Х298КО178', '7826010118795505': ' А288ХВ716', '7826010118795513': ' О847ХС198', '7826010118795521': ' О309ОТ198', '7826010118795539': ' Р579АК198', '7826010118795547': ' О403НН198', '7826010118795554': ' Е390ВО198', '7826010118795562': ' О585НК198', '7826010118795570': ' О779УН198', '7826010118795588': ' В319МА716', '7826010118795596': ' О585МУ198', '7826010118795604': ' Е329ВО198', '7826010118795612': ' Е752КС797', '7826010118795620': ' О845СО198', '7826010118795638': ' О465МУ198', '7826010118795646': ' Х297КО178', '7826010118795653': ' Х216МР198', '7826010118795661': ' В410РХ198', '7826010118795679': ' О218ОВ198', '7826010118795687': ' О577НМ198', '7826010118795695': ' Е255ВО198', '7826010118795703': ' О551МА198', '7826010118795711': ' О215КХ198', '7826010118795729': ' М901АА797', '7826010118795737': ' В174МВ716', '7826010118795745': ' О433УК198', '7826010118795752': ' О722ВВ198', '7826010118795760': ' В274МА716', '7826010118795778': ' В328КА198', '7826010118795786': ' А528ХВ716', '7826010118795794': ' О585НМ198', '7826010118795802': ' Х215МР178', '7826010118795810': ' О409НН198', '7826010118795828': ' А703ХВ716', '7826010118795836': ' Р240КМ797', '7826010118795844': ' О465КЕ198', '7826010118795851': ' В179СТ198', '7826010118795869': ' В588РХ198', '7826010118795877': ' В291КМ797', '7826010118795885': ' М065КЕ797', '7826010118795893': ' О969СТ198', '7826010118795901': ' А745ХВ716', '7826010118795919': ' О550НВ198', '7826010118795927': ' В723АТ797', '7826010118795935': ' Е392ВО198', '7826010118795943': ' О549КР198', '7826010118795950': ' Е217ВО198', '7826010118795968': ' В711РУ198', '7826010118795976': ' В483РХ198', '7826010118795984': ' О551ВЕ198', '7826010118795992': ' В184КР198', '7826010118796008': ' О402ОТ198', '7826010118796016': ' О855АУ198', '7826010118796024': ' О465ОЕ198', '7826010118796032': ' В324АЕ797', '7826010118796040': ' О815ВУ198', '7826010118796057': ' О551КН198', '7826010118796065': ' Р267АМ797', '7826010118796073': ' О403ВЕ198', '7826010118796081': ' Е767КС797', '7826010118796099': ' В273КМ797', '7826010118796107': ' О310ОМ198', '7826010118796115': ' Е754КК797', '7826010118796123': ' В567АС716', '7826010118796131': ' О218МН198', '7826010118796149': ' В655УХ198', '7826010118796156': ' О411ЕТ198', '7826010118796164': ' О496ХТ198', '7826010118796172': ' Е612НУ198', '7826010118796180': ' Е633ВС198', '7826010118796198': ' В168КР198', '7826010118796206': ' А266ХВ716', '7826010118796214': ' А745ХВ716', '7826010118796222': ' В717РУ198', '7826010118796230': ' Е231ВО198', '7826010118796248': ' В184КР797', '7826010118796255': ' В597РХ198', '7826010118796263': ' В179ТУ198', '7826010118796271': ' В604РО716', '7826010118796289': ' О257ЕУ198', '7826010118796297': ' В762РУ198', '7826010118796305': ' О410МО198', '7826010118796313': ' О526ВН198', '7826010118796321': ' Х659ОМ198', '7826010118796339': ' В316КМ797', '7826010118796347': ' В160КР198', '7826010118796354': ' О750КМ198', '7826010118796362': ' В038ХЕ198', '7826010118796370': ' В210КМ797', '7826010118796388': '', '7826010118796396': ' О309ОВ198', '7826010118796404': ' В254КР198', '7826010118796412': ' Х217МР178', '7826010118796420': ' В976РО716', '7826010118796438': ' О583НВ198', '7826010118796446': ' с866ау797', '7826010118796453': ' О576МН198', '7826010118796461': ' В236КА198', '7826010118796479': ' А863ХВ716', '7826010118796487': ' В149ТУ198', '7826010118796495': ' В263КМ797', '7826010118796503': ' Х127ЕТ797', '7826010118796511': ' О218КТ198', '7826010118796529': ' В061КР198', '7826010118796537': ' А735ХВ716', '7826010118796545': ' А830ХВ716', '7826010118796552': ' А606АХ797', '7826010118796560': ' В192СТ198', '7826010118796578': ' О218РВ198', '7826010118796586': ' О310МТ198', '7826010118796594': ' Е284ВО198', '7826010118796602': ' О218КМ198', '7826010118796610': ' е652ну198', '7826010118796628': ' О551МУ198', '7826010118796636': ' Е687ВС198', '7826010118796644': ' В133КР198', '7826010118796651': ' А318ХВ716', '7826010118796669': ' О258ЕТ198', '7826010118796677': ' О256ОМ198', '7826010118796685': ' Е680ВС198', '7826010118796693': ' А422ХВ716', '7826010118796701': ' О577МС198', '7826010118796719': ' О307КХ198', '7826010118796727': ' В167КР198', '7826010118796735': ' А605АС797', '7826010118796743': ' О193УК198', '7826010118796750': ' Е345ВО198', '7826010118796768': ' В290КА198', '7826010118796776': ' О403АС198', '7826010118796784': ' А286ХВ716', '7826010118796792': ' Х505ОМ178', '7826010118796800': ' О309ОС198', '7826010118796818': ' О551КМ198', '7826010118796826': ' Р103АР198', '7826010118796834': ' Р064АР198', '7826010118796842': '', '7826010118796859': ' О311НМ198', '7826010118796867': ' О218ое198', '7826010118796875': ' О310МО198', '7826010118796883': ' о310мт 198', '7826010118796891': 'Артамонова Олеся Владимировна; Е516МН198', '7826010118796909': ' У945КО797', '7826010118796917': ' Е964УЕ198', '7826010118796925': ' Е606НУ198', '7826010118796933': ' А628АМ797', '7826010118796941': '', '7826010118796958': '', '7826010118796966': '', '7826010118796974': '', '7826010118796982': '', '7826010118796990': '', '7826010118797006': '', '7826010118797014': '', '7826010118797022': '', '7826010118797030': '', '7826010118797048': ' 223', '7826010118797055': ' Е690ВС198', '7826010118797063': ' А808НТ147', '7826010118797071': ' О551МК198', '7826010118797089': ' е716ну198', '7826010118797097': ' О217ох198', '7826010118797105': ' е410во198', '7826010118797113': ' Р304Ам797', '7826010118797121': ' В827АЕ797', '7826010118797139': ' О748ВВ198', '7826010118797147': ' А075ХВ716', '7826010118797154': ' Р266ам797', '7826010118797162': ' О311НС198', '7826010118797170': ' С873АУ797', '7826010118797188': ' р819ао797', '7826010118797196': ' в740ас716', '7826010118797204': ' А624Ам797', '7826010118797212': ' О258ЕХ198', '7826010118797220': ' О436УС198', '7826010118797238': '', '7826010118797246': ' А926ХВ716', '7826010118797253': ' е612ну198', '7826010118797261': '', '7826010118797279': '', '7826010118797287': ' У210АУ98', '7826010118797295': ' Е425ВО198', '7826010118797303': ' в770ае797', '7826010118797311': ' о308но198', '7826010118797329': ' В170СТ198', '7826010118797337': '', '7826010118797345': '', '7826010118797352': '', '7826010118797360': '', '7826010118797378': '', '7826010118797386': ' х866та47', '7826010118797394': '', '7826010118797402': '', '7826010118797410': '', '7826010118797428': '', '7826010118797436': ' О624ЕМ198', '7826010118797444': ' С843АУ797', '7826010118797451': '', '7826010118797469': '', '7826010118797477': '', '7826010118797485': '', '7826010118797493': '', '7826010118797501': '', '7826010118797519': '', '7826010118797527': '', '7826010118797535': '', '7826010118797543': '', '7826010118797550': '', '7826010118797568': '', '7826010118797576': '', '7826010118797584': '', '7826010118797592': ' О452ХТ198', '7826010118797600': ' в116мв716', '7826010118797618': ' О454НТ198', '7826010118797626': ' В830АЕ797', '7826010118797634': ' О453ОУ198', '7826010118797642': ' О576ОА198', '7826010118797659': ' Е095ВН198', '7826010118797667': ' С834АУ797', '7826010118797675': '', '7826010118797683': '', '7826010118797691': '', '7826010118797709': '', '7826010118797717': '', '7826010118797725': '', '7826010118797733': '', '7826010118797741': '', '7826010118797758': '', '7826010118797766': '', '7826010118797774': '', '7826010118797782': '', '7826010118797790': '', '7826010118797808': '', '7826010118797816': '', '7826010118797824': '', '7826010118797832': '', '7826010118797840': ' А970ХВ716', '7826010118797857': ' М577КМ797', '7826010118797865': ' О430УМ198', '7826010118797873': ' О581МС198', '7826010118797881': ' Е793КК797', '7826010118797899': '', '7826010118797907': ' Е671НУ198', '7826010118797915': ' Р787АЕ797', '7826010118797923': ' Р278АО198', '7826010118797931': 'Водин Кирилл', '7826010118797949': '', '7826010118797956': '', '7826010118797964': ' В295КА198'}
    # wialon = WialonClient(0, 0)
    # wialon.login()
    # for card in cards.keys():
    #     if cards[card] != '':
    #         wialon.vehicle_search2(cards[card][:9], card)
    # wialon.logout()


baseURL = 'http://lk.tatneft.ru/solar-portal/api/'
login = 'AVENATOVSKIY@ATL.BIZ'
password = '21m03kla'
cards = get_cards(8502984)
transactions = get_transactions(8502984)
print(len(transactions))
print(transactions)
# print(transactions)
# get_cards_on_wialon()
# current_date = int(time.time()) + 10800
# current_day = datetime.utcfromtimestamp(current_date).strftime("%d")
# print(current_day)
# get_init(login=login, pwd=password, dateFrom='2022-10-31', dateTo='2022-12-01')
