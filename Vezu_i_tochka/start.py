from create_connection import create_connection
from download_files import download_files
from dim_drivers import upload_dim_drivers
from dim_cars import upload_dim_cars
from dim_clients import upload_dim_clients
from fact_waybills import upload_fact_waybills
from fact_payments import upload_fact_payments
from fact_rides import upload_fact_rides

connection_ufa = create_connection(  # Подключение к хранилищу данных
    'dwh', 'dwh_ufa', 'dwh_ufa_6x167KSn', 'de-edu-db.chronosavant.ru', '5432'
)

connection_origin = create_connection(  # Подключение к исходной базе данных
    'taxi', 'etl_tech_user', 'etl_tech_user_password', 'de-edu-db.chronosavant.ru', '5432'
)

path = 'c:/Users/maxim/Downloads/Hackaton/Files/'  # Путь к папке, в которую будут скачаны каталоги waybills и payments

download_files(path)  # Загрузка каталогов waybills и payments в указанную папку
upload_dim_drivers(connection_origin, connection_ufa)  # Импорт данных в таблицу dim_drivers
upload_dim_cars(connection_origin, connection_ufa)  # Импорт данных в таблицу dim_cars
upload_dim_clients(connection_origin, connection_ufa)  # Импорт данных в таблицу dim_clients
upload_fact_payments(connection_ufa, path)  # Импорт данных в таблицу fact_payments
upload_fact_waybills(connection_ufa, path)  # Импорт данных в таблицу fact_waybills
upload_fact_rides(connection_origin, connection_ufa)  # Импорт данных в таблицу fact_rides

connection_origin.close()
connection_ufa.close()
