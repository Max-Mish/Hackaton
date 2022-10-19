import glob
import os
import time
import xml.etree.ElementTree as ElementTree


def upload_fact_waybills(connection_upload, folder_path):
    # Функция, осуществляющая импорт данных в таблицу fact_payments
    connection_upload.autocommit = False
    folder_path = ''.join((folder_path, 'waybills/'))
    for filename in glob.glob(os.path.join(folder_path, '*.xml')):  # Обработка каждого файла из папки
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
        if len(driver_license_dt.split('.')) > 1:  # Преобразование некорректной даты
            lst = driver_license_dt.split('.')
            driver_license_dt = '-'.join((lst[2], lst[1], lst[0]))
        work_start_dt = root[0][3].find('start').text
        work_end_dt = root[0][3].find('stop').text
        issue_dt = root[0].get('issuedt')

        try:
            with connection_upload.cursor() as cursor:
                cursor.execute("""SET search_path TO dwh, dwh_ufa;""")  # Указываем путь к таблице
                cursor.execute(  # Получаем dim_pers_num из таблицы dim_drivers (Внешний ключ)
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
                cursor.execute("""SET search_path TO dwh, dwh_ufa;""")  # Указываем путь к таблице
                cursor.execute(  # Получаем car_plate_num из таблицы dim_cars (Внешний ключ)
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
                cursor.execute(  # Проверка на существование строки в таблице fact_waybills
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
                if cursor.fetchone()[0]:  # Если строки в таблице нет
                    cursor.execute(  # Добавляем строку в таблицу fact_waybills
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
