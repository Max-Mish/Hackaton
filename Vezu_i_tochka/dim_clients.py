def upload_dim_clients(connection_download, connection_upload):
    # Функция, осуществляющая импорт данных в таблицу dim_clients
    try:
        with connection_download.cursor() as cursor:
            cursor.execute("""SET search_path TO taxi, main;""")  # Указываем путь к таблице
            cursor.execute(  # Получаем данные из таблицы rides исходной БД
                """SELECT
                    client_phone, 
                    dt,
                    card_num 
                    FROM rides""")
            data = list(cursor.fetchall())  # Записываем их в data
    except Exception as e:
        print(f"The error '{e}' occurred")

    connection_upload.autocommit = False
    for row in data:  # Обработка каждой строки данных
        try:
            with connection_upload.cursor() as cursor:
                cursor.execute(  # Проверка на существование строки в таблице dim_cars
                    """
                    SELECT NOT EXISTS (SELECT * FROM dim_clients WHERE 
                        phone_num = (%s) AND
                        start_dt =(%s) AND
                        card_num=(%s)
                        )
                    """,
                    (row[0], row[1], row[2]))

                if cursor.fetchone()[0]:  # Если строки в таблице нет
                    cursor.execute(  # Получаем предыдущую строку с таким же номером телефона
                        """SELECT * FROM dim_clients WHERE
                            phone_num = (%s) AND
                            deleted_flag = (%s)
                        """,
                        (row[0], False))

                    if cursor.fetchall():  # Если такая предыдущая строка существует
                        cursor.execute(
                            # Обозначаем её как удалённую (deleted) и указываем время, когда она стала неактуальной
                            """UPDATE dim_clients SET
                                deleted_flag = (%s),
                                end_dt = (%s)
                                WHERE
                                phone_num = (%s) AND
                                deleted_flag = (%s)
                            """,
                            (True, row[1], row[0], False))
                        connection_upload.commit()

                    cursor.execute(  # Добавляем текущую строку в таблицу dim_clients
                        """
                        INSERT INTO dim_clients (
                            phone_num,
                            start_dt,
                            card_num,
                            deleted_flag
                            ) 
                        VALUES (%s, %s, %s, %s)
                        """,
                        (row[0], row[1], row[2], False))
                    connection_upload.commit()
                    print('Row uploaded')
        except Exception as e:
            print(f"The error '{e}' occurred")
