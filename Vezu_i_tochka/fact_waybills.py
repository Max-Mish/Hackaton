import glob
import os
import time
import xml.etree.ElementTree as ElementTree


def upload_fact_waybills(connection, path):
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
