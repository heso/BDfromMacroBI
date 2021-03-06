import psycopg2
import datetime


class ConnectionDBError(Exception):
    """Ошибка подключения к базе данных"""


class SQLError(Exception):
    """Не удалось выполнить запрос к базе данных"""


class MacroBIDB:

    def __init__(self, db_host: str, db_user: str, db_password: str, db_name: str) -> None:
        self.db_host = db_host
        self.db_user = db_user
        self.db_password = db_password
        self.db_name = db_name

    def __enter__(self):
        try:
            self.connection = psycopg2.connect(
                                                database=self.db_name,
                                                user=self.db_user,
                                                password=self.db_password,
                                                host=self.db_host
                                              )
            self.cursor = self.connection.cursor()
        except psycopg2.OperationalError as err:
            raise ConnectionDBError(err)
        except psycopg2.Error as err:
            raise ConnectionDBError(err)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()
        if exc_type is psycopg2.ProgrammingError:
            raise SQLError(exc_val)
        elif exc_type:
            raise exc_type(exc_val)

    def create_connection(self):
        return self.__enter__()

    def close_connection(self):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()

    def get_maximum_date(self, table_name: str) -> datetime:

        if table_name == 'Deals':
            date_field_name = 'agreement_date'
        else:
            date_field_name = 'date_added'

        query = f'SELECT MAX({date_field_name}) FROM {table_name}'

        try:
            self.cursor.execute(query)
            result = self.cursor.fetchone()[0]
        except (psycopg2.Error, ValueError):
            raise SQLError(f'Error to get maximum date from {table_name}')
        return result or datetime.datetime(2000, 1, 1)

    def create_table(self, table_name: str, captions: list, use_leads_or_deals: bool = False) -> None:
        table_captions = ', '.join(captions)
        if use_leads_or_deals:
            table_temp_captions = ', '.join(captions[:-3])
        else:
            table_temp_captions = ', '.join(captions)

        create_users_table = f'''
                                CREATE TABLE IF NOT EXISTS {table_name} ({table_captions})
                              '''
        create_users_table_temp = f'''
                                        DROP TABLE IF EXISTS {table_name}_temp; 
                                        CREATE TABLE IF NOT EXISTS {table_name}_temp ({table_temp_captions})
                                   '''
        try:
            self.cursor.execute(create_users_table)
        except psycopg2.Error:
            print("Error to create DB")

        try:
            self.cursor.execute(create_users_table_temp)
        except psycopg2.Error:
            raise SQLError("Error to create temp_DB")
        self.connection.commit()

    def insert_data(self, table_name: str, captions: list, data: list, use_deals: bool = False, use_leads: bool = False) -> None:
        captions = [x.split(' ')[0] for x in captions]
        if use_leads or use_deals:
            table_temp_captions = ', '.join(captions[:-3])
        else:
            table_temp_captions = ', '.join(captions)

        table_data = ", ".join(["%s"] * len(data))

        insert_query = (f'''
                            INSERT INTO {table_name}_temp ({table_temp_captions}) 
                            VALUES {table_data}; 
                         ''')

        if use_deals:
            insert_query += (f'''
                                UPDATE deals_temp
                                SET status_modified_date = d.status_modified_date 
                                FROM deals d
                                JOIN deals_temp dt on dt.id=d.id AND 
                                     dt.status=d.status AND 
                                     dt.is_payed_reserve=d.is_payed_reserve
                                WHERE deals_temp.id=d.id;
                          ''')

        insert_query += (f'''
                            DELETE FROM {table_name} 
                            WHERE {captions[0]} IN (SELECT {captions[0]} 
                                                    FROM {table_name}_temp);
                         ''')

        if use_leads or use_deals:
            insert_query += (f'''
                                INSERT INTO {table_name} 
                                SELECT t.*
                                       , tt.type_rus
                                       , COALESCE(h.complexname, 'Не распределено') complexname
                                       , COALESCE(h.housename, 'Не распределено') housename
                                FROM {table_name}_temp t 
                                LEFT JOIN types_translations tt ON t.category = tt.type_eng 
                                LEFT JOIN houses h ON t.id_house =  h.houseid
                                ''')
        else:
            insert_query += (f'''
                                INSERT INTO {table_name} 
                                SELECT * FROM {table_name}_temp 
                                ''')
        try:
            self.cursor.execute(insert_query,data)
        except (psycopg2.Error, ValueError):
            raise SQLError("Error to insert data to DB")
