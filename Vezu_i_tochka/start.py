from create_connection import create_connection
from download_files import download_files
from upload_files import *

connection = create_connection(
    'Vezu_i_tochka', 'postgres', 'admin', 'localhost', '5432'
)
path = 'c:/Users/maxim/Downloads/Hackaton/Files/'
download_files(path)
upload_payments(connection, path)
upload_waybills(connection, path)
connection.close()
