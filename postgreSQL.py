import psycopg2
import config_SQL as config
from accessify import private
import json


class PG_SQL:
    dbname = ''
    user = ''
    password = ''
    host = ''
    connection = None
    columns = {'cardNum': 'bigint',
               'drivers': 'text',
               'dates': 'int',
               'amounts': 'real',
               'prices': 'real',
               'sums': 'real',
               'posBrands': 'text',
               'latitude': 'real',
               'longitude': 'real',
               'posAddress': 'text',
               'serviceName': 'text'
               }

    def __init__(self, dbname, user, password, host):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host

    @private
    def _connect(self):
        try:
            self.connection = psycopg2.connect(dbname=self.dbname,
                                               user=self.user,
                                               password=self.password,
                                               host=self.host)
        except Exception as _ex_connect:
            print('Подключение к БД - ', _ex_connect)
        return self.connection

    @private
    def _disconnect(self):
        self.connection.close()

    def read_max_val_in_column(self, table, column, schema='', filters: dict = None):
        '''
        :param filters: фильтр AND по колонкам {'Название колонки': 'Искомое значение'}. Колонок для фильтрации может быть несколько.
        :param schema: схема для подключения к таблице
        :param table: наименование таблицы
        :param column: столбец, по которому произвести сортировку (от макс. к мин.)
        :return: максимальное значение в столбце
        '''
        self._connect()
        with self.connection.cursor() as cursor:
            try:
                sc = ''
                f = ''
                if schema != '':
                    sc = f'{schema}.'
                if filters is not None:
                    f = ' WHERE ' + ' AND '.join([f'{filter}={filters[filter]}' for filter in filters.keys()])
                command = f'SELECT {column} FROM {sc}{table}{f} ORDER BY {column} DESC'
                cursor.execute(command)
                row = cursor.fetchone()
            except Exception as _ex:
                print('Чтение строк - ', _ex)
        self._disconnect()
        try:
            return row[0]
        except TypeError:
            return 0
        except UnboundLocalError:
            return 0

    def read_rows(self, table, col_s=None, schema='', filters: dict = None):
        all_rows = []
        param = '*'
        sc = ''
        f = ''
        if schema != '':
            sc = f'{schema}.'
        if filters is not None:
            f = ' WHERE ' + ' AND '.join([f'{filter}={filters[filter]}' for filter in filters.keys()])
        if col_s is not None:
            if len(col_s) > 1:
                param = ', '.join(col_s)
            elif len(col_s) == 1:
                param = str(col_s[0])
        command = f'SELECT {param} FROM {sc}{table}{f}'
        self._connect()
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(command)
                all_rows = cursor.fetchall()
            except Exception as _ex_append_rows:
                print('Чтение строк - ', _ex_append_rows)
        self._disconnect()
        return all_rows

    def append_rows(self, table, rows, columns=None, schema=''):
        sc = ''
        if schema != '':
            sc = f'{schema}.'
        if columns is not None:
            col_s = f"({', '.join(columns)})"
        else:
            col_s = f"({', '.join([key for key in rows.keys()])})"
        count = len(rows[[key for key in rows.keys()][0]])
        # data = [tuple([str(value[i]) for value in rows.values()]) for i in range(count)]
        # rows_records = ', '.join(["%s"] * len(data))
        for i in range(count):
            rows_records = tuple([str(value[i]) for value in rows.values()])
            command = f'INSERT INTO {sc}{table}{col_s} VALUES {rows_records}'
            self._connect()
            with self.connection.cursor() as cursor:
                try:
                    cursor.execute(command)
                    self.connection.commit()
                    print(f'Строка {i+1} из {count} занесена в {table}')
                except Exception as _ex_append_rows:
                    pass
            self._disconnect()


if __name__ == '__main__':
    data = {'cardNum': [7826010103047375],
               'drivers': ['text'],
               'dates': [1675306920],
               'amounts': [137.88],
               'prices': [1.1],
               'sums': [1.1],
               'posBrands': ['text'],
               'latitude': [1.1],
               'longitude': [1.1],
               'posAddress': ['text'],
               'serviceName': ['text']
               }
    data1 = {'cardNum': [1, 2],
               'drivers': ['text', 'text2'],
               'dates': [71,1004],
               'amounts': [1.1, 1.2],
               'prices': [1.1, 1.2],
               'sums': [1.1, 1.2],
               'posBrands': ['text', 'text2'],
               'latitude': [1.1, 1.2],
               'longitude': [1.1, 1.2],
               'posAddress': ['text', 'text2'],
               'serviceName': ['text', 'text2']
            }
    sql = PG_SQL(dbname=config.db_name, user=config.user, host=config.host, password=config.password)
    test = {'cardNum': [1000888056, 1000870565], 'drivers': ['Коновалов Алексей Александрович', 'Филиппов Андрей Федорович'], 'dates': [1675508760, 1675433640], 'amounts': ['35.00', '40.00'], 'prices': ['50.37', '50.16'], 'sums': ['1762.83', '2006.35'], 'posBrands': ['Татнефть Великоустюгский р-н', 'Teboil Вологда'], 'latitude': [60.7587, 59.189953], 'longitude': [46.243, 39.854852], 'posAddress': ['Самотовинское с/п, п.Валга', 'Пошехонское ш., 38А, Вологда, Вологодская обл., Россия, 160022'], 'serviceName': ['АИ-92', 'АИ-92'], 'fuel_card_type': [13, 13], 'client_id': [1, 1]}
    sql.append_rows(table='refuelings', rows=test, schema='refuelings')
    # sql.append_rows_test(table='clients', rows=['А_Лайн'], columns=['name'], schema='refuelings')
    # sql.append_rows_test(table='fuel_cards_types', rows=['ППР', 1], columns=['name', 'client_id'], schema='refuelings')
    # print(sql.read_max_val_in_column('АТЛ', 'dates', 'АТЛ'))
    # sql.append_unique_rows('ИП_Денисов_Роснефть', data)
    # val = sql.read_max_val_in_column('АТЛ', 'dates')
    # print(val)
    # sql.create_schema('АТЛ')
