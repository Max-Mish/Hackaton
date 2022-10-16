import glob
import os
import time
import xml.etree.ElementTree as ElementTree


def upload_payments(connection, path):
    connection.autocommit = True

    with connection.cursor() as cursor:
        cursor.execute(
            """CREATE TABLE payments(
                id serial PRIMARY KEY,
                dt timestamp,
                card_number bigint,
                cost numeric(6, 2));"""
        )

    path = ''.join((path, 'payments/'))
    for filename in glob.glob(os.path.join(path, '*.csv')):
        with open(filename, 'r') as f:
            rows = f.readlines()
            for row in rows:
                row = list(row.strip().split())
                dt = ''.join((row[0], ' ', row[1]))
                card_number = row[2]
                cost = row[3]
                with connection.cursor() as cursor:
                    cursor.execute(
                        """INSERT INTO payments (dt, card_number, cost) VALUES (%s, %s, %s)""", (dt, card_number, cost))
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
