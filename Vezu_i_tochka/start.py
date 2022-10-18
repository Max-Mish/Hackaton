from create_connection import create_connection
from download_files import download_files
from dim_drivers import upload_dim_drivers
from dim_cars import upload_dim_cars
from fact_waybills import upload_fact_waybills
from fact_payments import upload_fact_payments

# connection_local = create_connection(
#     'Vezu_i_tochka', 'postgres', 'admin', 'localhost', '5432'
# )

connection_ufa = create_connection(
    'dwh', 'dwh_ufa', 'dwh_ufa_6x167KSn', 'de-edu-db.chronosavant.ru', '5432'
)

connection_origin = create_connection(
    'taxi', 'etl_tech_user', 'etl_tech_user_password', 'de-edu-db.chronosavant.ru', '5432'
)

path = 'c:/Users/maxim/Downloads/Hackaton/Files/'
# download_files(path)
# upload_payments(connection_ufa, path)
# upload_waybills(connection_ufa, path)
# upload_dim_drivers(connection_origin, connection_ufa)
# upload_dim_cars(connection_origin, connection_ufa)
# upload_dim_clients(connection_origin, connection_ufa)

connection_origin.close()
connection_ufa.close()
