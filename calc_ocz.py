import pandas as pd
import sqlalchemy as sa
import config as cfg
from datetime import datetime as dt
from time import time

time_start = time()
# Подключение к БД
ora = sa.create_engine('postgresql+psycopg2://' + cfg.user_db + ':' + cfg.pass_db + '@' + 'localhost/' + cfg.db)
conn = ora.connect()

# загружаем из базы ОЦЗ и ограничения
query_ocz = '''select dpg_code, target_date, hour, dir, volume
            from mfo_br_test.opcbids_dpgg_hour 
            where target_date = %(d)s
            order by dpg_code, target_date, hour'''
ocz = pd.read_sql(query_ocz, conn, params={'d': cfg.date})

query_reg = '''select dpg_code, target_date, hour, pmin_br, pmax_br
            from mfo_br_test.reg_dpg_date_hour 
            where target_date = %(d)s
            order by dpg_code, target_date, hour'''
reg = pd.read_sql(query_reg, conn, params={'d': cfg.date})
print(ocz)
print(reg)