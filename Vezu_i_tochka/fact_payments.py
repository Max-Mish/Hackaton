import glob
import os
import time


def upload_fact_payments(connection_upload, folder_path):
    connection_upload.autocommit = False
    folder_path = ''.join((folder_path, 'payments/'))
    for filename in glob.glob(os.path.join(folder_path, '*.csv')):
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
                    with connection_upload.cursor() as cursor:
                        cursor.execute(
                            """
                            SELECT NOT EXISTS (SELECT * FROM fact_payments WHERE
                                card_num = (%s) AND
                                transaction_amt = (%s) AND
                                transaction_dt = (%s)
                                )
                            """,
                            (card_num, transaction_amt, transaction_dt))
                        if cursor.fetchone()[0]:
                            cursor.execute(
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
