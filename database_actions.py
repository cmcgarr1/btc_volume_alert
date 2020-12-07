from mysql import connector
import get_btc_data
import os
from datetime import datetime
import send_text

db_user=os.environ['db_user']
db_password = os.environ['crypto_db_password']


def create_db():
    db_connection = connector.connect(
        host="localhost",
        user=db_user,
        passwd=db_password,
        auth_plugin='mysql_native_password'
    )
    # creating database_cursor to perform SQL operation
    db_cursor = db_connection.cursor()
    # executing cursor with execute method and pass SQL query

    db_cursor.execute("DROP DATABASE IF EXISTS bitcoin")

    db_cursor.execute("CREATE DATABASE bitcoin")
    # get list of all databases
    db_cursor.execute("SHOW DATABASES")
    # print all databases
    for db in db_cursor:
        print(db)
    create_btc_table('btc_price_data')


def create_btc_table(table_name):
    data_dictionary = {'last_updated': 'TIMESTAMP', 'volume_24h': 'BIGINT',
                       'market_cap': 'BIGINT', 'else': 'DECIMAL(15,8)'}

    db_connection = connect_to_database()
    db_cursor = db_connection.cursor()
    db_cursor.execute("DROP TABLE IF EXISTS "+table_name)

    a = get_btc_data.get_btc_price_data()
    column_header_string = '('
    for key in a:
        if key in data_dictionary:
            column_header_string = column_header_string + \
                ' '+key+' '+data_dictionary[key] + ','
        else:
            column_header_string = column_header_string + \
                ' '+key+' '+data_dictionary['else'] + ','
    column_header_string = column_header_string + \
        ' volume_percent_change DECIMAL(15,8), id INT AUTO_INCREMENT PRIMARY KEY)'
    db_cursor.execute("CREATE TABLE "+table_name+column_header_string)


def connect_to_database():
    db_connection = connector.connect(
        host="localhost",
        user=db_user,
        passwd=db_password,
        auth_plugin='mysql_native_password',
        database="bitcoin"
    )
    return db_connection


def convert_timestamp_to_datetime(timestamp_str):
    dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.000Z")
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def load_data(table_name):
    data_dictionary = {'volume_24h': 0, 'else': 4}
    db_connection = connect_to_database()
    db_cursor = db_connection.cursor()

    a = get_btc_data.get_btc_price_data()
    values_string = ' VALUES ('
    sql_insert_string = 'INSERT INTO '+table_name+' ('
    value_list = []

    for key in a:
        sql_insert_string = sql_insert_string+key+','
        values_string = values_string+'%s,'
        if type(a[key]) != str:
            if key in data_dictionary:
                value_list.append(round(a[key], data_dictionary[key]))
            else:
                value_list.append(round(a[key], data_dictionary['else']))
            # value_list.append(a[key])
        else:
            value_list.append(convert_timestamp_to_datetime(a[key]))

    sql_insert_string = sql_insert_string[:-1]+')'
    values_string = values_string[:-1]+');'
    db_cursor.execute(sql_insert_string+values_string, value_list)
    #db_cursor.execute('INSERT INTO btc_price_data (price,volume_24h,percent_change_1h,percent_change_24h,percent_change_7d,market_cap,last_updated) VALUES (%s,%s,%s,%s,%s,%s,%s)', value_list)
    db_connection.commit()

def update_single_volume_percent(table_name,time_frame):
    db_connection = connect_to_database()
    db_cursor = db_connection.cursor()

    db_cursor.execute("SELECT id, last_updated, volume_24h FROM "+table_name +
                      " WHERE id=(SELECT MAX(id) FROM "+table_name+")")
    last_volume_call=db_cursor.fetchall()

    db_cursor.execute("SELECT id, last_updated, volume_24h FROM "+table_name +
                      " WHERE id="+str(last_volume_call[0][0]-time_frame))
    previous_volume_call=db_cursor.fetchall()

    volume_percent_change=last_volume_call[0][2]/previous_volume_call[0][2]-1

    insert_string = "UPDATE "+table_name+" SET volume_percent_change=" + \
            str(volume_percent_change) + " where id="+str(last_volume_call[0][0])
    db_cursor.execute(insert_string)

    db_connection.commit()

def update_volume_moving_average(table_name,time_frame):
    db_connection = connect_to_database()
    db_cursor = db_connection.cursor()

    db_cursor.execute("SELECT id, volume_24h FROM "+table_name +
                      " WHERE id=(SELECT MAX(id) FROM "+table_name+")")
    last_volume_call=db_cursor.fetchall()

    db_cursor.execute("SELECT SUM(volume_24h) FROM "+table_name +
                      " WHERE id<"+str(last_volume_call[0][0])+" AND id >"+str(last_volume_call[0][0]-time_frame))
    previous_volume_call=db_cursor.fetchall()

    moving_average=previous_volume_call[0][0]/time_frame
    change_moving_average=((last_volume_call[0][1]/moving_average)-1)
    insert_string = "UPDATE "+table_name+" SET moving_average=" + \
            str(change_moving_average) + " where id="+str(last_volume_call[0][0])
    db_cursor.execute(insert_string)
    db_connection.commit()

def update_volume_percent_change_all(table_name,time_frame):
    db_connection = connect_to_database()
    db_cursor = db_connection.cursor()

    db_cursor.execute("SELECT id, last_updated, volume_24h FROM "+table_name +
                      " WHERE isNull(volume_percent_change) ORDER BY id ASC")
    missing_percent_change = db_cursor.fetchall()

    db_cursor.execute("SELECT id, last_updated, volume_24h FROM " +
                      table_name + " ORDER BY id ASC")
    all_data = db_cursor.fetchall()

    data_dictionary = {}
    for record in all_data:
        data_dictionary[record[0]] = record[2]

    for record in missing_percent_change:
        if record[0] <= time_frame:
            percent_change = 0
        else:
            percent_change = (
                data_dictionary[record[0]]/data_dictionary[record[0]-time_frame])-1

        insert_string = "UPDATE "+table_name+" SET volume_percent_change=" + \
            str(percent_change) + " where id="+str(record[0])
        db_cursor.execute(insert_string)

    db_connection.commit()

def get_last_volume_change(table_name, change_flag, use_moving_average=0):
    db_connection = connect_to_database()
    db_cursor = db_connection.cursor()

    if use_moving_average==1:
        db_cursor.execute("SELECT moving_average,price FROM "+table_name + \
                      " WHERE id=(SELECT MAX(id) FROM "+table_name+")")
    else:
        db_cursor.execute("SELECT volume_percent_change,price FROM "+table_name + \
                      " WHERE id=(SELECT MAX(id) FROM "+table_name+")")
    volume_percent_change = db_cursor.fetchall()

    if abs(volume_percent_change[0][0])>change_flag:
        send_text.send_text(volume_percent_change[0][0],volume_percent_change[0][1])


