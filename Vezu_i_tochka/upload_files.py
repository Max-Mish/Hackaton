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
                try:
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
                        connection.commit()
                except Exception as e:
                    print(f"The error '{e}' occurred")

    time.sleep(1)
    print('Payments successfully uploaded')


def upload_waybills(connection, path):
    connection.autocommit = True

    # with connection.cursor() as cursor:
    #     cursor.execute(
    #         """CREATE TABLE fact_waybills(
    #                         waybill_num char(6) PRIMARY KEY,
    #                         driver_pers_num int,
    #                         car_plate_num char(9),
    #                         work_start_dt timestamp(0),
    #                         work_end_dt timestamp(0),
    #                         issue_dt timestamp(0));"""
    #     )

    path = ''.join((path, 'waybills/'))
    for filename in glob.glob(os.path.join(path, '*.xml')):
        tree = ElementTree.parse(filename)
        root = tree.getroot()
        waybill_num = root[0].get('number')
        driver_pers_num = 'fk'
        car_plate_num = 'fk'
        car = root[0].find('car').text
        driver_name = root[0][2].find('name').text
        driver_license = root[0][2].find('license').text
        driver_valid_to = root[0][2].find('validto').text
        work_start_dt = root[0][3].find('start').text
        work_end_dt = root[0][3].find('stop').text
        issue_dt = root[0].get('issuedt')
        print(waybill_num, car, work_start_dt, work_end_dt, issue_dt)
        # with connection.cursor() as cursor:
            # cursor.execute(
                # """INSERT INTO waybills
                #    (car, model, driver_name, driver_license, driver_valid_to, period_start, period_stop)
                #    VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                # (car, model, driver_name, driver_license, driver_valid_to, period_start, period_stop))
    time.sleep(1)
    print('Waybills successfully uploaded')
