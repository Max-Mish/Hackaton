import glob
import os
import time
import xml.etree.ElementTree as ElementTree
from create_connection import create_connection


def upload_fact_waybills(connection_upload, path):
    connection_upload.autocommit = False
    path = ''.join((path, 'waybills/'))
    for filename in glob.glob(os.path.join(path, '*.xml')):
        tree = ElementTree.parse(filename)
        root = tree.getroot()

        waybill_num = root[0].get('number')
        plate_num = root[0].find('car').text
        model_name = root[0].find('model').text
        driver_name = root[0][2].find('name').text.split()
        last_name = driver_name[0]
        first_name = driver_name[1]
        middle_name = driver_name[2]
        driver_license_num = root[0][2].find('license').text
        driver_license_dt = root[0][2].find('validto').text
        if len(driver_license_dt.split('.')) > 1:
            lst = driver_license_dt.split('.')
            driver_license_dt = '-'.join((lst[2], lst[1], lst[0]))
        work_start_dt = root[0][3].find('start').text
        work_end_dt = root[0][3].find('stop').text
        issue_dt = root[0].get('issuedt')

        try:
            with connection_upload.cursor() as cursor:
                cursor.execute("""SET search_path TO dwh, dwh_ufa;""")
                cursor.execute(
                    """SELECT * FROM dim_drivers WHERE
                        last_name = (%s) AND
                        first_name = (%s) AND
                        middle_name = (%s) AND
                        driver_license_num = (%s) AND
                        driver_license_dt = (%s)
                    """,
                    (last_name, first_name, middle_name, driver_license_num, driver_license_dt))
                driver_pers_num = cursor.fetchall()[0][0]
        except Exception as e:
            print(f"The error '{e}' occurred")

        try:
            with connection_upload.cursor() as cursor:
                cursor.execute("""SET search_path TO dwh, dwh_ufa;""")
                cursor.execute(
                    """SELECT * FROM dim_cars WHERE
                        plate_num = (%s) AND
                        model_name = (%s)
                    """,
                    (plate_num, model_name))
                car_plate_num = cursor.fetchall()[0][0]
        except Exception as e:
            print(f"The error '{e}' occurred")

        try:
            with connection_upload.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT NOT EXISTS (SELECT * FROM fact_waybills WHERE
                        waybill_num = (%s) AND
                        driver_pers_num = (%s) AND
                        car_plate_num = (%s) AND
                        work_start_dt = (%s) AND
                        work_end_dt = (%s) AND
                        issue_dt = (%s)
                        )
                    """,
                    (waybill_num, driver_pers_num, car_plate_num, work_start_dt, work_end_dt, issue_dt))
                if cursor.fetchone()[0]:
                    cursor.execute(
                        """
                        INSERT INTO fact_waybills (
                            waybill_num,
                            driver_pers_num,
                            car_plate_num,
                            work_start_dt,
                            work_end_dt,
                            issue_dt
                            )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (waybill_num, driver_pers_num, car_plate_num, work_start_dt, work_end_dt, issue_dt))
                    print(f'{filename} uploaded')
                connection_upload.commit()
        except Exception as e:
            print(f"The error '{e}' occurred")
    time.sleep(1)
    print('Waybills successfully uploaded')


if __name__ == '__main__':
    connection_ufa = create_connection(
        'dwh', 'dwh_ufa', 'dwh_ufa_6x167KSn', 'de-edu-db.chronosavant.ru', '5432'
    )

    path = 'c:/Users/maxim/Downloads/Hackaton/Files/'
    upload_fact_waybills(connection_ufa, path)

    connection_ufa.close()
