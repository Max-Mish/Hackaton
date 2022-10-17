import glob
import os
import time
import xml.etree.ElementTree as ElementTree


def upload_payments(connection, path):
    connection.autocommit = True

    path = ''.join((path, 'payments/'))
    for filename in glob.glob(os.path.join(path, '*.csv')):
        with open(filename, 'r') as f:
            rows = f.readlines()
            for row in rows:
                row = list(row.strip().split())
                if len(row[0].split('.')) > 1:
                    lst = row[0].split('.')
                    row[0] = '-'.join((lst[2], lst[1], lst[0]))
                transaction_dt = ''.join((row[0], ' ', row[1]))
                card_num = row[2]
                transaction_amt = row[3]
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT NOT EXISTS (SELECT * FROM fact_payments WHERE card_num = (%s) AND transaction_amt = (%s) AND transaction_dt = (%s))
                        """,
                        (card_num, transaction_amt, transaction_dt))
                    if cursor.fetchone()[0]:
                        cursor.execute(
                            """
                            INSERT INTO fact_payments (card_num, transaction_amt, transaction_dt) VALUES (%s, %s, %s)
                            """,
                            (card_num, transaction_amt, transaction_dt))
                        print(f'{filename} uploaded')

    time.sleep(1)
    print('Payments successfully uploaded')


def upload_waybills(connection, path):
    connection.autocommit = True

    with connection.cursor() as cursor:
        cursor.execute(
            """CREATE TABLE waybills(
                            id serial PRIMARY KEY,
                            car char(9),
                            model varchar(30),
                            driver_name varchar(60),
                            driver_license char(12),
                            driver_valid_to date,
                            period_start timestamp(0),
                            period_stop timestamp(2));"""
        )

    path = ''.join((path, 'waybills/'))
    for filename in glob.glob(os.path.join(path, '*.xml')):
        tree = ElementTree.parse(filename)
        root = tree.getroot()
        car = root[0].find('car').text
        model = root[0].find('model').text
        driver_name = root[0][2].find('name').text
        driver_license = root[0][2].find('license').text
        driver_valid_to = root[0][2].find('validto').text
        period_start = root[0][3].find('start').text
        period_stop = root[0][3].find('stop').text
        with connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO waybills
                   (car, model, driver_name, driver_license, driver_valid_to, period_start, period_stop)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (car, model, driver_name, driver_license, driver_valid_to, period_start, period_stop))
    time.sleep(1)
    print('Waybills successfully uploaded')


if __name__ == '__main__':
    upload_payments()
