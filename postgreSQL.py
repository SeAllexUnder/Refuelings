import psycopg2
import config_SQL as config
from accessify import private


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

    def create_table(self, name, col_s=''):
        """
        :param name: название таблицы
        :param col_s: данные в формате: {"Название столбца": "Тип переменной"}
        :return: Except при наличии
        """
        if col_s == '':
            col_s = self.columns
        self._connect()
        col_s = ', '.join([key + ' ' + col_s[key] for key in col_s.keys()])
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(f"CREATE TABLE {name} ({col_s})")
                self.connection.commit()
            except Exception as _ex_create_table:
                print('Создание таблицы - ', _ex_create_table)
        self._disconnect()

    def read_rows(self, table, col_s=None):
        all_rows = []
        param = '*'
        if col_s is not None:
            if len(col_s) > 1:
                param = ', '.join(col_s)
            elif len(col_s) == 1:
                param = str(col_s[0])
        self._connect()
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(f'SELECT {param} FROM {table}')
                all_rows = cursor.fetchall()
            except Exception as _ex_append_rows:
                print('Чтение строк - ', _ex_append_rows)
        self._disconnect()
        return all_rows

    def append_rows(self, table, rows):
        rows_records = ', '.join(["%s"] * len(rows))
        command = f'INSERT INTO {table} VALUES {rows_records}'
        self._connect()
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(command, rows)
                self.connection.commit()
            except Exception as _ex_append_rows:
                print('Занесение строк - ', _ex_append_rows)
        self._disconnect()

    def append_unique_rows(self, table, _data, check=True):
        """
        :param check: проверка дубликатов перед занесением. По умолчанию включена
        :param table: наименование таблицы
        :param _data: данные в формате: {"Наименование столбца": [Список значений в столбце]}
        :return: None
        """
        if check:
            buffer = f'Buf_{table}'
        else:
            buffer = table
        self.create_table(buffer)
        col_s = '(' + ', '.join(_data.keys()) + ')'
        count = len(_data[[key for key in _data.keys()][0]])
        rows = [tuple([str(value[i]) for value in _data.values()]) for i in range(count)]
        if len(rows) == 0:
            self._disconnect()
            exit()
        self.append_rows(buffer, rows)
        if check:
            self.delete_dublicates(buffer, table)

    def read_max_val_in_column(self, table, column):
        '''
        :param table: наименование таблицы
        :param column: столбец, по которому произвести сортировку (от макс. к мин.)
        :return: максимальное значение в столбце
        '''
        self._connect()
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(f'SELECT {column} FROM {table} ORDER BY {column} DESC')
                row = cursor.fetchone()
            except Exception as _ex:
                print('Чтение строк - ', _ex)
        self._disconnect()
        try:
            return row[0]
        except TypeError:
            return 0

    def delete_dublicates(self, buffer, table):
        command = f'SELECT * FROM {buffer} UNION SELECT * FROM {table}'
        self._connect()
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(command)
                unique_rows = cursor.fetchall()
            except Exception as _ex:
                print('Объединение таблиц - ', _ex)
        self._disconnect()
        self.clear_table(table)
        self.append_rows(table, unique_rows)
        self.delete_table(buffer)

    def clear_table(self, table):
        command = f'TRUNCATE {table}'
        self._connect()
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(command)
                self.connection.commit()
            except Exception as _ex:
                print('Очистка таблицы - ', _ex)
        self._disconnect()

    def delete_table(self, table):
        command = f'DROP TABLE {table}'
        self._connect()
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(command)
                self.connection.commit()
            except Exception as _ex:
                print('Удаление таблицы - ', _ex)
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
    name = 'ИП_Денисов_Роснефть'
    # sql.create_table(name)
    sql.append_unique_rows('ИП_Денисов_Роснефть', data)
    # val = sql.read_max_val_in_column('АТЛ', 'dates')
    # print(val)
