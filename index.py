import os
os.chdir('/home/pi/git/volume_alert')
import database_actions

table_name='btc_price_data'
database_actions.load_data(table_name)
database_actions.update_singe_volume_percent(table_name,3)
database_actions.get_last_volume_change(table_name, 0.05)

