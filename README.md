# CENTRAL SET CLI


```python
import pandas as pd
import datetime
from central_set_cli import CentralSet, ETLReportBase
```

# Initialize the main class and Login


```python
cs = CentralSet(host = 'http://localhost:8080')\
    .connect()\
    .login()
```


```python
cs.token
```



# APP / DATABSE


```python
cs.get_apps()
lmbd = lambda app: {'app_id': app['app_id'], 'app': app['app'], 'db': app['db']}
apps = list(map(lmbd, cs.apps))
apps
```




    [{'app_id': 1, 'app': 'ADMIN', 'db': 'ADMIN_DB'},
     {'app_id': 5, 'app': 'TESOURARIA', 'db': 'TESOURARIA'},
     {'app_id': 13, 'app': 'SA_v21', 'db': 'SA_v21'}]




```python
cs.set_app('ADMIN')
cs.app
```




    {'app_id': 1,
     'app': 'ADMIN',
     'app_desc': 'Admin app',
     'version': '1.0.0',
     'email': 'adm@domain.com',
     'db': 'ADMIN_DB',
     'attach_logo': 'favicon.ico',
     'config': '{}',
     'user_id': 1,
     'created_at': '2023-11-18T11:22:37.460265',
     'updated_at': '2023-11-18T11:22:37.460277',
     'excluded': False}



# SELECT DB TABLES


```python
cs.get_tables()
tables = cs.tables
for table in tables:
    pass
    #print(f"{table}: {tables[table]['comment']}")
```

# CREATE


```python
_data = {
    'app_id': 1,
    'app': 'ADMIN',
    'app_desc': 'Admin app',
    'version': '1.0.0',
    'email': 'adm@domain.com',
    'db': 'ADMIN_DB',
    'attach_logo': 'favicon.ico'
 }
#res = cs.create_params({'table': 'app', 'data': _data})\
#        .create()
#print(res)
```

# READ


```python
data = cs.read_params({'table': 'app', 'limit': -1, 'fields': ['app_id', 'app', 'app_desc']})\
        .read()\
        .get('data')
print(data)
```

    [{'app_id': 1, 'app': 'ADMIN', 'app_desc': 'Admin app'}, {'app_id': 5, 'app': 'TESOURARIA', 'app_desc': ''}, {'app_id': 13, 'app': 'SA_v21', 'app_desc': 'SA_v21'}]


# UPLOAD FILE


```python
file_path = '/path/to/file/file_name.extention'
#res = cs.upload({'tmp': False}, file_path)
#print(res)
```

# ETL / REPORT / BASE


```python
etl = ETLReportBase(cs)
```

## GET THE LIST OF AVALIABLE ETLs / REPORTS / BASES


```python
_data = etl.get_etl_report_base(pattern = 'tax')
_fn = lambda e: {'id': e.get('etl_report_base_id'), 'name': e.get('etl_report_base'), 'database': e.get('database')}
def _fn(e):
    return {
        'id': e.get('etl_report_base_id'), 
        'name': e.get('etl_report_base'), 
        'database': e.get('database')
    }
list(map(_fn, _data))
```




    [{'id': 11, 'name': 'NYC_TAXI', 'database': 'NYC_TAXI.duckdb'}]



## GET DATA (INPUT | OUTPUTS | ...) ETL / REPORT / BASE


```python
item = {'id': 11, 'name': 'NYC_TAXI', 'database': 'NYC_TAXI.duckdb'}
etl.get_data(etl_report_base = item)
etl_report_base = etl.data['etl_report_base'].get('data', [])[0]
etl_report_base
```




    {'etl_report_base_id': 11,
     'etl_report_base': 'NYC_TAXI',
     'etl_report_base_desc': 'Teste DUCKDB Devs',
     'attach_etl_rbase_doc': None,
     'periodicity_id': 1,
     'database': 'NYC_TAXI.duckdb',
     'includes_data_quality': True,
     'includes_data_reconci': True,
     'includes_exports': True,
     'etl_report_base_conf': None,
     'active': True,
     'user_id': 1,
     'app_id': 1,
     'created_at': '2023-11-11T18:13:03.122978',
     'updated_at': '2023-11-13T15:23:23.152889',
     'excluded': False,
     'includes_notify': True}



## SET / GET DATE REF


```python
etl.ref
```




    datetime.date(2023, 11, 17)




```python
etl.set_ref('2023-10-31').ref
```




    datetime.date(2023, 10, 31)



## RUN STEPS


```python
etl.inputs()\
   .outputs()\
   .data_quality()\
   .data_reconcilia()\
   .export()\
   .notify()
```

    RUNNING: extract...
    RUNNING: extract/taxi_2019_04...
    RUNNING: extract/taxi_2019_05...
    FINISHING: extract...
    RUNNING: transform...
    RUNNING: transform/Teste...
    FINISHING: transform...
    RUNNING: data_quality...
    RUNNING: data_quality/VT0001...
    FINISHING: data_quality...
    RUNNING: data_reconcilia...
    RUNNING: data_reconcilia/TaxyDiffs...
    FINISHING: data_reconcilia...
    RUNNING: export...
    RUNNING: export/DumpTesteOutput2CSV...
    RUNNING: export/DumpTesteOutput2Parquet...
    FINISHING: export...
    RUNNING: notify...
    RUNNING: notify/TAXY YYYYMMDD...
    FINISHING: notify...





    <src.central_set_cli.ETLReportBase at 0x7f1e42d7ffa0>



## RUN ALL STEPS


```python
etl.run_all()
```

    RUNNING: extract...
    RUNNING: extract/taxi_2019_04...
    RUNNING: extract/taxi_2019_05...
    FINISHING: extract...
    RUNNING: transform...
    RUNNING: transform/Teste...
    RUNNING: transform/Teste2...
    FINISHING: transform...
    RUNNING: data_quality...
    RUNNING: data_quality/VT0001...
    FINISHING: data_quality...
    RUNNING: data_reconcilia...
    RUNNING: data_reconcilia/TaxyDiffs...
    FINISHING: data_reconcilia...
    RUNNING: export...
    RUNNING: export/DumpTesteOutput2CSV...
    RUNNING: export/DumpTesteOutput2Parquet...
    FINISHING: export...
    RUNNING: notify...
    RUNNING: notify/TAXY YYYYMMDD...
    FINISHING: notify...





    <src.central_set_cli.ETLReportBase at 0x7fdafc532700>



## STEPS ALLOW AND SKIP CONF


```python
allow_skip_conf: dict = {
    "extract": {
        "skip": True,
        "allow_specifics": None,
        "skip_specifics": None
    },
    "transform": {
        "skip": False,
        "allow_specifics": None,
        "skip_specifics": None
    },
    "data_quality": {
        "skip": False,
        "allow_specifics": None,
        "skip_specifics": None
    },
    "data_reconcilia": {
        "skip": False,
        "allow_specifics": None,
        "skip_specifics": None
    },
    "export": {
        "skip": False,
        "allow_specifics": None,
        "skip_specifics": ["DumpTesteOutput2CSV"]
    },
    "notify": {
        "skip": False,
        "allow_specifics": None,
        "skip_specifics": None
    }
}
etl.set_allow_skip_conf(allow_skip_conf)\
    .run_all()
```

    RUNNING: extract...
    RUNNING: transform...
    RUNNING: transform/Teste...
    FINISHING: transform...
    RUNNING: data_quality...
    RUNNING: data_quality/VT0001...
    FINISHING: data_quality...
    RUNNING: data_reconcilia...
    RUNNING: data_reconcilia/TaxyDiffs...
    FINISHING: data_reconcilia...
    RUNNING: export...
    RUNNING: export/DumpTesteOutput2CSV...
    RUNNING: export/DumpTesteOutput2Parquet...
    FINISHING: export...
    RUNNING: notify...
    RUNNING: notify/TAXY YYYYMMDD...
    FINISHING: notify...





    <src.central_set_cli.ETLReportBase at 0x7f3f34b86f10>



## LOGS


```python
etl.save_logs()
```


```python
logs = pd.DataFrame(etl.get_logs())
_fields = ['type', 'ref', 'name', 'start', 'end', 'success', 'msg', 'timer']
logs.shape
```




    (6, 13)




```python
logs[_fields].head()
```

## SAVE THE LOGS


```python
logs = etl.get_logs()
def map_transf(log):
    for k in log:
        if isinstance(log[k], datetime.date):
            log[k] = log[k].strftime('%Y-%m-%d')
        elif isinstance(log[k], datetime.datetime):
            log[k] = log[k].strftime('%Y-%m-%d %H:%m:%s')
        return log
logs = list(map(map_transf, logs))
res = cs.create_params({'table': 'etl_report_base_log', 'data': logs})\
        .create()
print(res)
```

    {'success': True, 'msg': 'Operation executed successfully!', 'data': {'etl_report_base_log_row_exec_1': {'success': True, 'msg': 'Operation executed successfully!', 'pk': 'log_id', 'inserted_primary_key': 32, 'data': {'type': 'TRANSFORM', 'ref': '2023-10-31', 'name': 'Teste', 'start': '2023-11-18T19:40:31.022129', 'success': True, 'msg': 'Operation executed successfully!', 'num_rows': None, 'num_cols': None, 'fname': None, 'end': '2023-11-18T19:40:35.214374', 'timer': '4.19s', 'etl_report_base_id': 11, 'log_id': 32}, 'sql': 'INSERT INTO etl_report_base_log (type, name, ref, start, "end", timer, success, msg, num_rows, fname, etl_report_base_id, user_id, created_at, updated_at, excluded) VALUES (:type, :name, :ref, :start, :end, :timer, :success, :msg, :num_rows, :fname, :etl_report_base_id, :user_id, :created_at, :updated_at, :excluded)'}, 'etl_report_base_log_row_exec_2': {'success': True, 'msg': 'Operation executed successfully!', 'pk': 'log_id', 'inserted_primary_key': 33, 'data': {'type': 'DATA_RECONCILIA', 'ref': '2023-10-31', 'name': 'TaxyDiffs', 'start': '2023-11-18T19:40:35.440510', 'success': True, 'msg': 'Operation executed successfully!', 'num_rows': None, 'num_cols': None, 'fname': None, 'end': '2023-11-18T19:40:35.725139', 'timer': '0.28s', 'etl_report_base_id': 11, 'html': None, 'log_id': 33}, 'sql': 'INSERT INTO etl_report_base_log (type, name, ref, start, "end", timer, success, msg, num_rows, fname, etl_report_base_id, user_id, created_at, updated_at, excluded) VALUES (:type, :name, :ref, :start, :end, :timer, :success, :msg, :num_rows, :fname, :etl_report_base_id, :user_id, :created_at, :updated_at, :excluded)'}, 'etl_report_base_log_row_exec_3': {'success': True, 'msg': 'Operation executed successfully!', 'pk': 'log_id', 'inserted_primary_key': 34, 'data': {'type': 'DATA_RECONCILIA', 'ref': '2023-10-31', 'name': 'TaxyDiffs', 'start': '2023-11-18T19:40:35.440510', 'success': True, 'msg': 'Operation executed successfully!', 'num_rows': None, 'num_cols': None, 'fname': None, 'end': '2023-11-18T19:40:35.725139', 'timer': '0.28s', 'etl_report_base_id': 11, 'html': None, 'log_id': 34}, 'sql': 'INSERT INTO etl_report_base_log (type, name, ref, start, "end", timer, success, msg, num_rows, fname, etl_report_base_id, user_id, created_at, updated_at, excluded) VALUES (:type, :name, :ref, :start, :end, :timer, :success, :msg, :num_rows, :fname, :etl_report_base_id, :user_id, :created_at, :updated_at, :excluded)'}, 'etl_report_base_log_row_exec_4': {'success': True, 'msg': 'Operation executed successfully!', 'pk': 'log_id', 'inserted_primary_key': 35, 'data': {'type': 'EXPORT', 'ref': '2023-10-31', 'name': 'DumpTesteOutput2CSV', 'start': '2023-11-18T19:40:35.725593', 'success': False, 'msg': 'Interrupted by configuration', 'end': '2023-11-18T19:40:35.725613', 'timer': '0.00s', 'etl_report_base_id': 11, 'log_id': 35}, 'sql': 'INSERT INTO etl_report_base_log (type, name, ref, start, "end", timer, success, msg, etl_report_base_id, user_id, created_at, updated_at, excluded) VALUES (:type, :name, :ref, :start, :end, :timer, :success, :msg, :etl_report_base_id, :user_id, :created_at, :updated_at, :excluded)'}, 'etl_report_base_log_row_exec_5': {'success': True, 'msg': 'Operation executed successfully!', 'pk': 'log_id', 'inserted_primary_key': 36, 'data': {'type': 'EXPORT', 'ref': '2023-10-31', 'name': 'DumpTesteOutput2Parquet', 'start': '2023-11-18T19:40:35.725634', 'success': False, 'msg': "Unexpected error [Errno 2] No such file or directory: '/home/clovis/Documents/apps/fast-api/static/uploads/tmp'", 'num_rows': None, 'num_cols': None, 'fname': None, 'end': '2023-11-18T19:40:36.036306', 'timer': '0.31s', 'etl_report_base_id': 11, 'log_id': 36}, 'sql': 'INSERT INTO etl_report_base_log (type, name, ref, start, "end", timer, success, msg, num_rows, fname, etl_report_base_id, user_id, created_at, updated_at, excluded) VALUES (:type, :name, :ref, :start, :end, :timer, :success, :msg, :num_rows, :fname, :etl_report_base_id, :user_id, :created_at, :updated_at, :excluded)'}, 'etl_report_base_log_row_exec_6': {'success': True, 'msg': 'Operation executed successfully!', 'pk': 'log_id', 'inserted_primary_key': 37, 'data': {'type': 'NOTIFY', 'ref': '2023-10-31', 'name': 'TAXY YYYYMMDD', 'start': '2023-11-18T19:40:36.036506', 'success': False, 'msg': 'Unexpected error [Errno -3] Temporary failure in name resolution', 'num_rows': None, 'num_cols': None, 'fname': None, 'end': '2023-11-18T19:40:36.224203', 'timer': '0.19s', 'etl_report_base_id': 11, 'log_id': 37}, 'sql': 'INSERT INTO etl_report_base_log (type, name, ref, start, "end", timer, success, msg, num_rows, fname, etl_report_base_id, user_id, created_at, updated_at, excluded) VALUES (:type, :name, :ref, :start, :end, :timer, :success, :msg, :num_rows, :fname, :etl_report_base_id, :user_id, :created_at, :updated_at, :excluded)'}}}

