def upload_dim_cars(connection_download, connection_upload):
    # Функция, осуществляющая импорт данных в таблицу dim_cars
    try:
        with connection_download.cursor() as cursor:
            cursor.execute("""SET search_path TO taxi, main;""")  # Указываем путь к таблице
            cursor.execute(  # Получаем данные из таблицы car_pool исходной БД
                """SELECT 
                    plate_num, 
                    model, 
                    revision_dt,
                    update_dt
                    FROM car_pool""")
            data = list(cursor.fetchall())  # Записываем их в data
    except Exception as e:
        print(f"The error '{e}' occurred")

    connection_upload.autocommit = False
    for row in data:  # Обработка каждой строки данных
        try:
            with connection_upload.cursor() as cursor:
                cursor.execute(  # Проверка на существование строки в таблице dim_cars
                    """
                    SELECT NOT EXISTS (SELECT * FROM dim_cars WHERE 
                        plate_num = (%s) AND
                        model_name = (%s) AND
                        revision_dt = (%s) AND
                        start_dt = (%s)
                        )
                    """,
                    (row[0], row[1], row[2], row[3]))
                if cursor.fetchone()[0]:  # Если строки в таблице нет
                    cursor.execute(  # Добавляем строку в таблицу dim_cars
                        """
                        INSERT INTO dim_cars (
                            plate_num, 
                            start_dt, 
                            model_name,
                            revision_dt,
                            deleted_flag
                            ) 
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (row[0], row[3], row[1], row[2], False))
                    connection_upload.commit()
                    print('Row uploaded')
        except Exception as e:
            print(f"The error '{e}' occurred")
