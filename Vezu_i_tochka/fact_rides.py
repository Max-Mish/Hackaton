from create_connection import create_connection


def upload_fact_rides(connection_download, connection_upload):
    connection_upload.autocommit = False
    try:
        with connection_download.cursor() as cursor:
            cursor.execute("""SET search_path TO taxi, main;""")
            cursor.execute(
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
            data = list(cursor.fetchall())

    except Exception as e:
        print(f"The error '{e}' occurred")

    for row in data:
        row = list(row) + [None] * 6

        try:
            with connection_upload.cursor() as cursor:
                cursor.execute("""SET search_path TO dwh, dwh_ufa;""")
                cursor.execute(
                    """SELECT phone_num FROM dim_clients WHERE
                        phone_num = (%s) AND
                        card_num = (%s)
                    """, (row[2], row[3]))
                row[8] = list(cursor.fetchall())[0][0].strip()
        except Exception as e:
            print(f"The error '{e}' occurred")

        del row[2:4]

        try:
            with connection_download.cursor() as cursor:
                cursor.execute("""SET search_path TO taxi, main;""")
                cursor.execute(
                    """SELECT car_plate_num, event, dt FROM main.movement WHERE 
                        ride = (%s)
                    """, (row[0],))
                for t in list(cursor.fetchall()):
                    if t[0] not in row:
                        row[8] = t[0]
                    if t[1] == 'READY':
                        row[9] = t[2]
                    elif t[1] == 'BEGIN':
                        row[10] = t[2]
                    elif t[1] == 'END':
                        row[11] = t[2]
                    elif t[1] == 'CANCEL':
                        row[11] = t[2]
        except Exception as e:
            print(f"The error '{e}' occurred")

        try:
            with connection_upload.cursor() as cursor:
                cursor.execute("""SET search_path TO dwh, dwh_ufa;""")
                cursor.execute(
                    """SELECT plate_num FROM dim_cars WHERE
                        plate_num = (%s)
                    """, (row[8],))
                row[8] = list(cursor.fetchall())[0][0].strip()
        except Exception as e:
            print(f"The error '{e}' occurred")

        try:
            with connection_upload.cursor() as cursor:
                cursor.execute("""SET search_path TO dwh, dwh_ufa;""")
                cursor.execute(
                    """SELECT driver_pers_num FROM fact_waybills WHERE
                        work_start_dt <= (%s) AND
                        work_end_dt > (%s) AND
                        car_plate_num = (%s)
                    """, (row[1], row[1], row[8]))
                row[7] = list(cursor.fetchall())[0][0]
        except Exception as e:
            print(f"The error '{e}' occurred")

        del row[1]

        try:
            with connection_upload.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT NOT EXISTS (SELECT * FROM fact_rides WHERE
                        ride_id = (%s) AND
                        point_from_txt = (%s) AND
                        point_to_txt = (%s) AND
                        distance_val = (%s) AND
                        price_amt = (%s) AND 
                        client_phone_num = (%s) AND
                        drivers_pers_num = (%s) AND 
                        car_plate_num = (%s) AND 
                        ride_arrival_dt = (%s) AND 
                        ride_start_dt = (%s) AND 
                        ride_end_dt = (%s)
                        )
                    """,
                    (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10]))
                if cursor.fetchone()[0]:
                    cursor.execute(
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


if __name__ == '__main__':
    connection_ufa = create_connection(
        'dwh', 'dwh_ufa', 'dwh_ufa_6x167KSn', 'de-edu-db.chronosavant.ru', '5432'
    )

    connection_origin = create_connection(
        'taxi', 'etl_tech_user', 'etl_tech_user_password', 'de-edu-db.chronosavant.ru', '5432'
    )

    upload_fact_rides(connection_origin, connection_ufa)

    connection_origin.close()
    connection_ufa.close()
