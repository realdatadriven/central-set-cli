"""Central Set Python CLI"""
# pylint: disable = broad-exception-caught
# pylint: disable = unused-import
import re
import os
from dataclasses import dataclass, asdict
from typing import Optional
import copy
import datetime
import json
import sys
import time
from dateutil import parser
import requests
from dotenv import load_dotenv
load_dotenv()

@dataclass
class ReadParams:
    '''READ DATA STRUCT'''
    table: str = None
    tables: list = None
    database: str = None
    limit: int = 10
    offset: int = 0
    fields: Optional[list] = None
    filters: Optional[list] = None
    order_by: Optional[list] = None
    pattern: Optional[str] = None
    distinct: Optional[bool] = False
    join: str = 'all'
    def get_dict(self) -> dict:
        '''returns the field in the dict format'''
        return {k: v for k, v in asdict(self).items()}
@dataclass
class CreateParams:
    '''CREATE DATA STRUCT'''
    table: str = None
    data: dict = None
    database: str = None
    def get_dict(self) -> dict:
        '''returns the field in the dict format'''
        return {k: v for k, v in asdict(self).items()}
@dataclass
class Init:
    """Initialize"""
    host: str = 'localhost'
    # port: Optional[int] = 80
    user: str = os.environ.get('CS_USER')
    password: str = os.environ.get('CS_PASS')
    lang: str = 'en'
    token: str = os.environ.get('CS_TOKEN')
    apps: list = None
    app: dict = None
    tables: dict = None
    _r_params: ReadParams = None
    _c_params: CreateParams = None
    def api_call(self, api: str, payload: dict):
        '''API CALL'''
        try:
            headers = {'Accept': '*/*', 'Content-type': 'application/json'}#, 'Accept': 'text/plain'}
            if self.token:
                headers['Authorization'] = f'Bearer {self.token}'
            if self.app and isinstance(payload, dict):
                payload['app'] = self.app
            if self.lang and isinstance(payload, dict):
                payload['lang'] = self.lang
            r = requests.post(
                url     = api,
                data    = json.dumps(payload),
                headers = headers,
                verify  = False,
                timeout = 1000
            )
            return r.json()
        except Exception as _err:
            *_, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('API CALL ERR: ', str(_err), fname, exc_tb.tb_lineno)
            return {
                'success': False,
                'msg': f'API Call Err: {str(_err)}'
            }
    def set_lang(self, lang: str = 'en'):
        '''LANG'''
        self.lang = lang
        return self
    def login(self, user: str = None, password: str = None):
        '''LOGIN'''
        if user:
            self.user = user
        if password:
            self.password = password
        api = f'{self.host}/dyn_api/login/login'
        payload = {'data': {'username': self.user, 'password': self.password}}
        res = self.api_call(api, payload)
        if res.get('success'):
            self.token = res.get('token')
        else:
            raise TypeError(res.get('message'))
        return self
    def get_apps(self):
        '''GET APP'''
        api = f'{self.host}/dyn_api/admin/apps'
        res = self.api_call(api, {})
        if res.get('success'):
            self.apps = res.get('data')
            self.set_app(self.apps[0]['app'])
        else:
            raise TypeError(res.get('message'))
        return self
    def get_tables(self, table:str = None):
        '''GET TABLES'''
        api = f'{self.host}/dyn_api/admin/tables'
        res = self.api_call(api, {'table': table})
        if res.get('success'):
            self.tables = res.get('data')
        else:
            raise TypeError(res.get('message'))
        return self
    def set_app(self, app: str):
        '''SET APP'''
        if not self.apps:
            raise TypeError('Run .get_apps first')
        elif len(self.apps) == 0:
            raise TypeError('Run .get_apps first')
        _app = list(filter(lambda a: a.get('app') == app or a.get('app_id') == app, self.apps))
        if len(_app) == 0:
            raise TypeError(f'The app {app} doesn\'t seem to exist!')
        self.app = _app[0]
        return self
    def read_params(self, params):
        '''return read parameters'''
        if params:
            self._r_params = ReadParams(**params)
        return self
    def read(self, payload: ReadParams = None):
        '''READ'''
        if payload:
            self._r_params = payload
        api = f'{self.host}/dyn_api/crud/read'
        return self.api_call(api, {'data': self._r_params.get_dict()})
    def create_params(self, params):
        '''return create parameters'''
        if params:
            self._c_params = CreateParams(**params)
        return self
    def create(self, payload: CreateParams = None):
        '''CREATE'''
        if payload:
            self._c_params = payload
        api = f'{self.host}/dyn_api/crud/create'
        return self.api_call(api, {'data': self._c_params.get_dict()})
    def update(self, payload: CreateParams = None):
        '''UPDATE'''
        if payload:
            self._c_params = payload
        api = f'{self.host}/dyn_api/crud/update'
        return self.api_call(api, {'data': self._c_params.get_dict()})
    def delete(self, payload: CreateParams = None):
        '''DELETE'''
        if payload:
            self._c_params = payload
        api = f'{self.host}/dyn_api/crud/delete'
        return self.api_call(api, {'data': self._c_params.get_dict()})
    def upload(self, payload: dict, file_path:str):
        '''UPLOAD FILE'''
        post_files = {
            "file": open(file_path, "rb"),
        }
        api = f'{self.host}/upload'
        headers = {
            "Accept": "*/*",
            "enctype": "multipart/form-data",
            "Authorization": f"Bearer {self.token}",
            #"Content-Type": "multipart/form-data; boundary=kljmyvW1ndjXaOEAg4vPm6RBUqO6MC5A" 
        }
        res = requests.request(
            "POST",
            api,
            data = payload,
            files = post_files,
            headers = headers,
            timeout= 1000
        )
        return res.json()
class CentralSet():
    """Central Set Python CLI"""
    def __init__(self, host: str = None, user: str = None, password: str = None):
        self.host = host
        # self.port = port
        self.user = user
        self.password = password
    def connect(self, host: str = None, user: str = None, password: str = None):
        '''CONNECT'''
        init = Init()
        if host:
            init.host = host
        elif self.host:
            init.host = self.host
        if user:
            init.user = user
        elif self.user:
            init.user = self.user
        if password:
            init.password = password
        elif self.password:
            init.password = self.password
        return init
class ETLReportBase():
    '''Process ETL / REPORT / DATABASE'''
    tables: list = [
        'etl_report_base', 
        'etl_rbase_input', 
        'etl_rbase_output', #'etl_rb_output_field',
        'etl_rbase_quality', 
        'etl_rb_reconcilia', 
        'etl_rb_reconc_dtail',
        'etl_rbase_export', #'etl_rb_exp_dtail',
        'etl_report_base_log',
        'etl_rbase_notify'
    ]
    steps: list = [
        {
            "table": "etl_rbase_input",
            "label": "Extract | Input",
            "date_ref": "YYYY-MM-DD",
            "run_all_action": "extract"
        },
        {
            "table": "etl_rbase_output",
            "detail_table": "etl_rb_output_field",
            "label": "Transform | Output",
            "dates_refs": [],
            "run_all_action": "transform"
        },
        {
            "table": "etl_rbase_quality",
            "label": "Data Quality",
            "show": "selected_etlrb?.includes_data_quality",
            "dates_refs": [],
            "run_all_action": "data_quality"
        },
        {
            "table": "etl_rb_reconcilia",
            "detail_table": "etl_rb_reconc_dtail",
            "label": "Reconciliation",
            "show": "selected_etlrb?.includes_data_reconci",
            "dates_refs": [],
            "run_all_action": "data_reconcilia"
        },
        {
            "table": "etl_rbase_export",
            "detail_table": "etl_rb_exp_dtail",
            "label": "Export | Template",
            "show": "selected_etlrb?.includes_exports",
            "dates_refs": [],
            "run_all_action": "export"
        },
        {
            "table": "etl_rbase_notify",
            "label": "Alert | Notification",
            "show": "selected_etlrb?.includes_notify",
            "name": "notify_subject",
            "dates_refs": [],
            "run_all_action": "notify"
        }
    ]
    data: dict = None,
    ref: datetime.date = datetime.datetime.now().date() - datetime.timedelta(days = 1)
    log: dict = {}
    allow_skip_conf: dict = {
        "extract": {
            "skip": False,
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
            "skip_specifics": None
        },
        "notify": {
            "skip": False,
            "allow_specifics": None,
            "skip_specifics": None
        }
    }
    def __init__(self, cs:Init = None, steps: list = None):
        self.cs = Init() if not cs else cs
        self.steps = self.steps if not steps else steps
    def set_tables(self, tables: list):
        '''get ETL / REPORT / DATABASE tables'''
        if tables:
            self.tables = tables
        return self
    def set_steps(self, steps: list):
        '''get ETL / REPORT / DATABASE tables'''
        if steps:
            self.steps = steps
        return self
    def set_ref(self, ref):
        '''SET DATE REF'''        
        if not ref:
            pass
        elif isinstance(ref, str):
            self.ref = parser.parse(ref).date()
        elif isinstance(ref, list):           
            if isinstance(ref[0], datetime.date):
                self.ref =  ref[0]
            else:
                self.ref = parser.parse(ref[0]).date()
        elif isinstance(ref, datetime.date):
            self.ref = ref
        elif isinstance(ref, datetime.datetime):
            self.ref = ref.date()
        return self
    def get_etl_report_base(self, pattern: str = None):
        '''GET avaliable ETL / REPORT / DATABASE'''
        _params = {
            'table': self.tables[0], 
            'limit': -1, 
            'join': 'none',
            'pattern': pattern
        }
        return self.cs.read_params(_params).read().get('data')
    def get_data(self, etl_report_base: dict, tables: list = None, ref: datetime.date = None):
        '''get ETL / REPORT / DATABASE Data Inputs | Outputs | ...'''
        if tables:
            self.set_tables(tables)
        if ref:
            self.set_ref(ref)
        _params = {
            'table': self.tables, 
            'limit': -1, 
            'join': 'none',
            'filters': [
                {
                    'field': 'etl_report_base_id', 
                    'cond': '=', 
                    'value': etl_report_base.get('etl_report_base_id', etl_report_base.get('id'))
                },
                {
                    'field': 'ref', 
                    'cond': 'LIKE', 
                    'value': self.ref.strftime('%Y-%m-%d')
                }
            ]
        }
        self.data = self.cs.read_params(_params).read().get('data')
        return self
    def get_timer(self, start, end):
        '''get the start end time diff str'''
        timer = ''
        if (end - start) < 60:
            timer = '{:.2f}s'.format((end - start))
        else:
            timer = '{:.2f}m'.format((end - start)/60)
        return timer
    def set_allow_skip_conf(self, allow_skip_conf: dict):
        '''SET ALLOW AND SKIP CONFIG'''
        if allow_skip_conf:
            self.allow_skip_conf = allow_skip_conf
        return self
    def run_step(self, step, data = None, ref = None):
        '''RUN STEP'''
        if not ref:
            ref = self.ref.strftime('%Y-%m-%d')
        elif isinstance(ref, (datetime.date, datetime.datetime)):
            ref = ref.strftime('%Y-%m-%d')
        elif isinstance(ref, list):
            ref = [
                rf.strftime('%Y-%m-%d') if isinstance(ref, (datetime.date, datetime.datetime))
                else rf for rf in ref
            ]
        _actions = ['transform', 'data_quality', 'data_reconcilia', 'export', 'notify']
        if step.get("run_all_action") in _actions:
            if not isinstance(ref, list):
                ref = [ ref ]
        if step.get('dates_refs') and not isinstance(ref, list):
            ref = [ ref ]
        label = f'RUNNING: {step.get("run_all_action")}...'
        print(label)
        _conf = self.allow_skip_conf.get(step.get("run_all_action"), {})
        if _conf.get('skip') is True:
            return self
        api = f'{self.cs.host}/dyn_api/etl/{step.get("run_all_action")}'
        self.log =  {
            'type': step.get("run_all_action").upper(),
            'ref': ref if isinstance(ref, str) else ';'.join(ref),
        }
        if not data:
            data = self.data[step.get("table")].get('data', [])
        selected_etlrb = self.data['etl_report_base'].get('data', [])[0]
        if not self.data['etl_report_base_log']:
            self.data['etl_report_base_log'] = {'data': []}
        elif not self.data['etl_report_base_log'].get('data'):
            self.data['etl_report_base_log']['data'] = []
        if isinstance(data, list) and step.get("run_all_action") not in ['data_quality']:
            for d in data:
                if step.get("run_all_action") in ['extract'] and not d.get('etl_rbase_input_conf'):
                    continue
                if d.get('active') is False:
                    continue
                self.log =  {
                    'type': step.get("run_all_action").upper(),
                    'ref': ref if isinstance(ref, str) else ';'.join(ref),
                    'name': d.get(step.get('name', step.get('table'))),
                    'start': datetime.datetime.now().isoformat(),
                }
                label = f'RUNNING: {step.get("run_all_action")}/{self.log["name"]}...'
                print(label)
                start = time.time()
                # ONLY ALLOWED
                if not _conf.get('allow_specifics'):
                    pass
                elif not isinstance(_conf.get('allow_specifics'), list):
                    pass
                elif len(_conf.get('allow_specifics')) == 0:
                    pass
                elif self.log['name'] not in _conf.get('allow_specifics'):
                    self.log['success'] = False
                    self.log['msg'] = 'Interrupted by configuration'
                    self.log['end'] = datetime.datetime.now().isoformat()
                    end = time.time()
                    self.log['timer'] = self.get_timer(start, end)
                    self.log['etl_report_base_id'] = selected_etlrb.get('etl_report_base_id')
                    self.data['etl_report_base_log']['data'].append(copy.deepcopy(self.log))
                    continue
                # SKIP NOT ALLOWED
                if not _conf.get('skip_specifics'):
                    pass
                elif not isinstance(_conf.get('skip_specifics'), list):
                    pass
                elif len(_conf.get('skip_specifics')) == 0:
                    pass
                elif self.log['name'] in _conf.get('skip_specifics'):
                    self.log['success'] = False
                    self.log['msg'] = 'Interrupted by configuration'
                    end = time.time()
                    self.log['end'] = datetime.datetime.now().isoformat()
                    self.log['timer'] = self.get_timer(start, end)
                    self.log['etl_report_base_id'] = selected_etlrb.get('etl_report_base_id')
                    self.data['etl_report_base_log']['data'].append(copy.deepcopy(self.log))
                    continue
                payload = {
                    'step': {**step, 'date_ref': ref, 'dates_refs': ref},
                    'data': {**d, 'date_ref': ref},
                    'selected_etlrb': selected_etlrb,
                    'date_ref': ref,
                    'db_data': self.data if step.get("run_all_action") == 'notify' else None
                }
                _aux = self.cs.api_call(api, {'data': payload})
                self.log['success'] = _aux.get('success')
                self.log['msg'] = _aux.get('msg')
                self.log['num_rows'] = _aux.get('n_rows')
                self.log['num_cols'] = _aux.get('n_cols')
                self.log['fname'] = _aux.get('fname')
                end = time.time()
                self.log['end'] = datetime.datetime.now().isoformat()
                self.log['timer'] = self.get_timer(start, end)
                self.log['etl_report_base_id'] = selected_etlrb.get('etl_report_base_id')
                if not _aux.get('data'): # HANDLE MULTILINE FEEDBACK
                    self.data['etl_report_base_log']['data'].append(copy.deepcopy(self.log))
                elif step.get("run_all_action") in ['export']:
                    _data = _aux.get('data')
                    for _d in _data:
                        self.log['ref'] = _d.get('date_ref') if _d.get('date_ref') else self.log['ref']
                        self.log['msg'] = _d.get('msg') if _d.get('msg') else self.log['msg']
                        self.log['fname'] = _d.get('fname')
                        self.data['etl_report_base_log']['data'].append(copy.deepcopy(self.log))
                elif step.get("run_all_action") in ['data_reconcilia']:
                    _data = _aux.get('data')
                    for _d in _data:
                        self.log['ref'] = _d.get('date_ref') if _d.get('date_ref') else self.log['ref']
                        self.log['msg'] = _d.get('msg') if _d.get('msg') else self.log['msg']
                        self.log['html'] = _d.get('html')
                        self.data['etl_report_base_log']['data'].append(copy.deepcopy(self.log))
        else:
            if not data:
                return self
            elif isinstance(data, list) and step.get("run_all_action") in ['data_quality']:
                if len(data) > 0:
                    data = data[0]
                else:
                    return self
            if step.get("run_all_action") in ['extract'] and not data.get('etl_rbase_input_conf'):
                return self
            if data.get('active') is False:
                return self
            self.log =  {
                'type': step.get("run_all_action").upper(),
                'ref': ref if isinstance(ref, str) else ';'.join(ref),
                'name': data.get(step.get('name', step.get('table'))),
                'start': datetime.datetime.now().isoformat(),
            }
            label = f'RUNNING: {step.get("run_all_action")}/{self.log["name"]}...'
            print(label)
            start = time.time()
            # ONLY ALLOWED
            if not _conf.get('allow_specifics'):
                pass
            elif not isinstance(_conf.get('allow_specifics'), list):
                pass
            elif len(_conf.get('allow_specifics')) == 0:
                pass
            elif self.log['name'] not in _conf.get('allow_specifics'):
                self.log['success'] = False
                self.log['msg'] = 'Interrupted by configuration'
                end = time.time()
                self.log['end'] = datetime.datetime.now().isoformat()
                self.log['timer'] = self.get_timer(start, end)
                self.log['etl_report_base_id'] = selected_etlrb.get('etl_report_base_id')
                self.data['etl_report_base_log']['data'].append(copy.deepcopy(self.log))
                return self
            # SKIP NOT ALLOWED
            if not _conf.get('skip_specifics'):
                pass
            elif not isinstance(_conf.get('skip_specifics'), list):
                pass
            elif len(_conf.get('skip_specifics')) == 0:
                pass
            elif self.log['name'] in _conf.get('skip_specifics'):
                self.log['success'] = False
                self.log['msg'] = 'Interrupted by configuration'
                end = time.time()
                self.log['end'] = datetime.datetime.now().isoformat()
                self.log['timer'] = self.get_timer(start, end)
                self.log['etl_report_base_id'] = selected_etlrb.get('etl_report_base_id')
                self.data['etl_report_base_log']['data'].append(copy.deepcopy(self.log))
                return self
            payload = {
                'step': {**step, 'date_ref': ref, 'dates_refs': ref},
                'data': {**data, 'date_ref': ref},
                'selected_etlrb': selected_etlrb,
                'date_ref': ref,
                'db_data': self.data if step.get("run_all_action") == 'notify' else None
            }
            _aux = self.cs.api_call(api, {'data': payload})
            self.log['success'] = _aux.get('success')
            self.log['msg'] = _aux.get('msg')
            self.log['num_rows'] = _aux.get('n_rows')
            self.log['num_cols'] = _aux.get('n_cols')
            self.log['fname'] = _aux.get('fname')
            self.log['end'] = datetime.datetime.now().isoformat()
            end = time.time()
            self.log['timer'] = self.get_timer(start, end)
            self.log['etl_report_base_id'] = selected_etlrb.get('etl_report_base_id')
            if not _aux.get('data'): # HANDLE MULTILINE FEEDBACK
                self.data['etl_report_base_log']['data'].append(copy.deepcopy(self.log))
            elif step.get("run_all_action") in ['data_quality']:
                _data = _aux.get('data')
                for _dq in self.data[step.get("table")].get('data', []):
                    if _data['check'].get(_dq.get('etl_rbase_quality_id')):
                        self.log['name'] = _dq.get('etl_rbase_quality')
                        self.log['errors'] = _data['check'].get(_dq.get('etl_rbase_quality_id'))
                        self.log['fixes'] = _data['fix'].get(_dq.get('etl_rbase_quality_id'))
                        self.data['etl_report_base_log']['data'].append(copy.deepcopy(self.log))
            elif step.get("run_all_action") in ['export']:
                _data = _aux.get('data')
                for _d in _data:
                    self.log['ref'] = _d.get('date_ref') if _d.get('date_ref') else self.log['ref']
                    self.log['msg'] = _d.get('msg') if _d.get('msg') else self.log['msg']
                    self.log['fname'] = _d.get('fname')
                    self.data['etl_report_base_log']['data'].append(copy.deepcopy(self.log))
            elif step.get("run_all_action") in ['data_reconcilia']:
                _data = _aux.get('data')
                for _d in _data:
                    self.log['ref'] = _d.get('date_ref') if _d.get('date_ref') else self.log['ref']
                    self.log['msg'] = _d.get('msg') if _d.get('msg') else self.log['msg']
                    self.log['html'] = _d.get('html')
                    self.data['etl_report_base_log']['data'].append(copy.deepcopy(self.log))
        label = f'FINISHING: {step.get("run_all_action")}...'
        print(label)
        return self
    def run_filtered(self, _filter: str, ref = None):
        '''IILTERED'''
        _step = list(filter(lambda s: s.get('run_all_action') == _filter, self.steps))
        return self.run_step(_step[0], ref)
    def inputs(self, ref = None):
        '''RUN INPUTS | EXTRACTIONS'''
        return self.run_filtered('extract', ref)
    def outputs(self, ref = None):
        '''RUN OUTPUTS | TRANSFORMS'''
        return self.run_filtered('transform', ref)
    def data_quality(self, ref = None):
        '''RUN DATA QUALITY'''
        return self.run_filtered('data_quality', ref)
    def data_reconcilia(self, ref = None):
        '''RUN DATA RECONCILIATION'''
        return self.run_filtered('data_reconcilia', ref)
    def export(self, ref = None):
        '''RUN DATA EXPORT'''
        return self.run_filtered('export', ref)
    def notify(self, ref = None):
        '''RUN DATA NOTIFY'''
        return self.run_filtered('notify', ref)
    def run_all(self, ref = None):
        '''RUN ALL'''
        for step in self.steps:
            self.run_step(step, ref)
        return self
    def get_logs(self):
        '''return the logs of the execution'''
        return self.data['etl_report_base_log'].get('data')
    def save_logs(self):
        '''SAVE THE LOGS GENERATE DURING THE PROCESSING'''
        logs = self.get_logs()
        res = self.cs.create_params({'table': 'etl_report_base_log', 'data': logs})\
                    .create()
        print(res.get('msg'))
        return self

