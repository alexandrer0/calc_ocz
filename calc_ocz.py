import pandas as pd
import sqlalchemy as sa
import config as cfg
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

# print(ocz.dtypes)
# print(reg.dtypes)
corr_ocz = ocz.merge(reg, on=['dpg_code', 'target_date', 'hour'])

# Считаем ОЦЗ
corr_ocz_plus = corr_ocz.drop(corr_ocz[corr_ocz['dir'] != 2].index, axis=0)
corr_ocz_minus = corr_ocz.drop(corr_ocz[corr_ocz['dir'] != 1].index, axis=0)
corr_ocz_plus.drop(corr_ocz_plus[corr_ocz_plus['volume'] <= corr_ocz_plus['pmin_br']].index, axis=0, inplace=True)
corr_ocz_minus.drop(corr_ocz_minus[corr_ocz_minus['volume'] >= corr_ocz_minus['pmax_br']].index, axis=0, inplace=True)
corr_ocz_plus['volume'] = pd.DataFrame([corr_ocz_plus['volume'], corr_ocz_plus['pmax_br']]).min()
corr_ocz_minus['volume'] = pd.DataFrame([corr_ocz_minus['volume'], corr_ocz_minus['pmin_br']]).max()
colum = ['pmin_br', 'pmax_br']
corr_ocz_plus.drop(colum, axis=1, inplace=True)
corr_ocz_minus.drop(colum, axis=1, inplace=True)
corr_ocz = corr_ocz_plus.append(corr_ocz_minus)
print(corr_ocz)

# Запись в базу
corr_ocz.to_sql('calcopc_dpg_date_hour', conn, 'mfo_br_test', if_exists='replace', index=False, chunksize=200,
          dtype={'dpg_code': sa.types.VARCHAR(10)})

print(round(time() - time_start, 2), 'sec')

conn.close()

