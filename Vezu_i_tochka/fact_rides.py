def upload_fact_rides(connection_download, connection_upload):
    # Функция, осуществляющая импорт данных в таблицу fact_rides
    connection_upload.autocommit = False
    try:
        with connection_download.cursor() as cursor:
            cursor.execute("""SET search_path TO taxi, main;""")  # Указываем путь к таблице
            cursor.execute(  # Получаем данные из таблицы rides исходной БД
                """SELECT 
                    ride_id,
                    dt, 
                    client_phone,
                    card_num,
                    point_from, 
                    point_to,
                    distance,
                    price
                    FROM main.rides""")
            data = list(cursor.fetchall())  # Записываем их в data

    except Exception as e:
        print(f"The error '{e}' occurred")

    for row in data:  # Обработка каждой строки данных
        row = list(row) + [None] * 6  # Дополняем список пустыми элементами до количества столбцов в таблице

        try:
            with connection_upload.cursor() as cursor:
                cursor.execute("""SET search_path TO dwh, dwh_ufa;""")  # Указываем путь к таблице
                cursor.execute(  # Получаем phone_num из таблицы dim_clients (Внешний ключ)
                    """SELECT phone_num FROM dim_clients WHERE
                        phone_num = (%s) AND
                        card_num = (%s)
                    """, (row[2], row[3]))
                row[8] = list(cursor.fetchall())[0][0].strip()
        except Exception as e:
            print(f"The error '{e}' occurred")

        del row[2:4]  # Удаляем из строки ненужные данные

        try:
            with connection_download.cursor() as cursor:
                cursor.execute("""SET search_path TO taxi, main;""")  # Указываем путь к таблице
                cursor.execute(  # Получаем данные из таблицы movement исходной БД
                    """SELECT car_plate_num, event, dt FROM main.movement WHERE 
                        ride = (%s)
                    """, (row[0],))
                for t in list(cursor.fetchall()):
                    if t[0] not in row:  # Добавляем car_plate_num в строку
                        row[8] = t[0]
                    if t[1] == 'READY':  # Добавляем ride_arrival_dt в строку
                        row[9] = t[2]
                    elif t[1] == 'BEGIN':  # Добавляем ride_start_dt в строку
                        row[10] = t[2]
                    elif t[1] == 'END':  # Добавляем ride_end_dt в строку если поездка завершена
                        row[11] = t[2]
                    elif t[1] == 'CANCEL':  # Добавляем ride_end_dt в строку если поездка отменена
                        row[11] = t[2]
        except Exception as e:
            print(f"The error '{e}' occurred")

        try:
            with connection_upload.cursor() as cursor:
                cursor.execute("""SET search_path TO dwh, dwh_ufa;""")  # Указываем путь к таблице
                cursor.execute(  # Получаем plate_num из таблицы dim_cars (Внешний ключ)
                    """SELECT plate_num FROM dim_cars WHERE
                        plate_num = (%s)
                    """, (row[8],))
                row[8] = list(cursor.fetchall())[0][0].strip()
        except Exception as e:
            print(f"The error '{e}' occurred")

        try:
            with connection_upload.cursor() as cursor:
                cursor.execute("""SET search_path TO dwh, dwh_ufa;""")  # Указываем путь к таблице
                cursor.execute(  # Получаем driver_pers_num из таблицы fact_waybills (Внешний ключ)
                    """SELECT driver_pers_num FROM fact_waybills WHERE
                        work_start_dt <= (%s) AND
                        work_end_dt > (%s) AND
                        car_plate_num = (%s)
                    """, (row[1], row[1], row[8]))
                row[7] = list(cursor.fetchall())[0][0]
        except Exception as e:
            print(f"The error '{e}' occurred")

        del row[1]  # Удаляем из строки ненужные данные

        try:
            with connection_upload.cursor() as cursor:
                cursor.execute(  # Проверка на существование строки в таблице fact_rides
                    """
                    SELECT NOT EXISTS (SELECT * FROM fact_rides WHERE
                        ride_id = (%s) AND
                        point_from_txt = (%s) AND
                        point_to_txt = (%s) AND
                        distance_val = (%s) AND
                        price_amt = (%s) AND 
                        client_phone_num = (%s) AND
                        drivers_pers_num = (%s) AND 
                        car_plate_num = (%s)
                        )
                    """,
                    (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]))
                if cursor.fetchone()[0]:  # Если строки в таблице нет
                    cursor.execute(  # Добавляем строку в таблицу fact_rides
                        """
                        INSERT INTO fact_rides (
                            ride_id,
                            point_from_txt,
                            point_to_txt,
                            distance_val,
                            price_amt,
                            client_phone_num,
                            drivers_pers_num,
                            car_plate_num,
                            ride_arrival_dt,
                            ride_start_dt,
                            ride_end_dt
                            )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]))
                    connection_upload.commit()
                    print('Row uploaded')
        except Exception as e:
            print(f"The error '{e}' occurred")
