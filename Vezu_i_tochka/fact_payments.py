import glob
import os
import time


def upload_fact_payments(connection_upload, folder_path):
    # Функция, осуществляющая импорт данных в таблицу fact_payments
    connection_upload.autocommit = False
    folder_path = ''.join((folder_path, 'payments/'))

    try:
        with connection_upload.cursor() as cursor:
            cursor.execute(  # Сбрасываем автоинкремент, чтобы при обновлении таблицы transaction_id выдавался корректно
                """ALTER SEQUENCE fact_payments_transaction_id_seq RESTART WITH 1;
                    UPDATE fact_payments SET transaction_id=nextval('fact_payments_transaction_id_seq');"""
            )
        connection_upload.commit()
    except Exception as e:
        print(f"The error '{e}' occurred")

    for filename in glob.glob(os.path.join(folder_path, '*.csv')):  # Обработка каждого файла из папки
        with open(filename, 'r') as f:
            rows = f.readlines()
            for row in rows:  # Обработка каждой строки данных из файла
                row = list(row.strip().split())
                if len(row[0].split('.')) > 1:  # Преобразование некорректной даты
                    lst = row[0].split('.')
                    row[0] = '-'.join((lst[2], lst[1], lst[0]))
                transaction_dt = ''.join((row[0], ' ', row[1]))
                card_num = row[2]
                transaction_amt = row[3]
                try:
                    with connection_upload.cursor() as cursor:
                        cursor.execute(  # Проверка на существование строки в таблице fact_payments
                            """
                            SELECT NOT EXISTS (SELECT * FROM fact_payments WHERE
                                card_num = (%s) AND
                                transaction_amt = (%s) AND
                                transaction_dt = (%s)
                                )
                            """,
                            (card_num, transaction_amt, transaction_dt))
                        if cursor.fetchone()[0]:  # Если строки в таблице нет
                            cursor.execute(  # Добавляем строку в таблицу fact_payments
                                """
                                INSERT INTO fact_payments (
                                    card_num,
                                    transaction_amt,
                                    transaction_dt
                                    )
                                VALUES (%s, %s, %s)
                                """,
                                (card_num, transaction_amt, transaction_dt))
                            print(f'{filename} uploaded')
                        connection_upload.commit()
                except Exception as e:
                    print(f"The error '{e}' occurred")

    time.sleep(1)
    print('Payments successfully uploaded')
