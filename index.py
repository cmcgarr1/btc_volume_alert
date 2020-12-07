import os
os.chdir('/home/pi/git/btc_volume_alert')
import database_actions
import datetime

table_name='btc_price_data'

print(datetime.datetime.now())
database_actions.load_data(table_name)
database_actions.update_single_volume_percent(table_name,3)
database_actions.update_volume_moving_average(table_name,24)
database_actions.get_last_volume_change(table_name, 0.1,1)

