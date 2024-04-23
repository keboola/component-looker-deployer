'''
Template Component main class.

'''
import logging
import os
import subprocess
from pathlib import Path
import csv
import sys
import json
import requests
from urllib.request import pathname2url  # noqa
import urllib.parse
import pandas as pd
from datetime import datetime

from keboola.component import CommonInterface

# configuration variables

# #### Keep for debug
KEY_DEBUG = 'debug'
KEY_MODE = 'mode'
KEY_FROM = 'from'
KEY_TO = 'to'
KEY_BASE_URL = 'base_url'
KEY_CLIENT_ID = 'client_id'
KEY_CLIENT_SECRET = '#client_secret'
KEY_CREDENTIALS = [
    KEY_BASE_URL,
    KEY_CLIENT_ID,
    KEY_CLIENT_SECRET
]

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [
    KEY_FROM,
    KEY_TO
]
REQUIRED_IMAGE_PARS = []

CURRENT_DATE = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

APP_VERSION = '0.0.6'


def get_local_data_path():
    return Path(__file__).resolve().parent.parent.joinpath('data').as_posix()


def get_data_folder_path():
    data_folder_path = None
    if not os.environ.get('KBC_DATADIR'):
        data_folder_path = get_local_data_path()
    return data_folder_path


class Component(CommonInterface):
    def __init__(self):
        # for easier local project setup
        # data_folder_path = get_data_folder_path()
        # super().__init__(data_folder_path=data_folder_path)
        super().__init__()

        try:
            # validation of required parameters. Produces ValueError
            self.validate_configuration(REQUIRED_PARAMETERS)
            self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        except ValueError as e:
            logging.exception(e)
            exit(1)

        if self.configuration.parameters.get(KEY_DEBUG):
            self.set_debug_mode()

    @staticmethod
    def set_debug_mode():
        logging.getLogger().setLevel(logging.DEBUG)
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

    def run(self):
        '''
        Main execution code
        '''
        params = self.configuration.parameters

        # Validate user parameters
        self.validate_user_params(params)

        # FROM Parameters
        from_params = params.get(KEY_FROM)

        # TO Parameters
        to_params = params.get(KEY_TO)

        # Application mode
        mode = params.get(KEY_MODE)
        logging.info(f'Mode: [{mode}]')

        # caching dashboard details for deployment
        # there had been issues where new dashboards have misaligned ids
        self.all_dashboards = {
            'from': {},
            'to': {}
        }

        # caching looks details for deployment
        self.all_looks = {
            'from': {},
            'to': {}
        }

        # Details for FROM
        if from_params['base_url']:
            self.fetch_details(from_params, 'from')

        # Details for TO
        if to_params['base_url']:
            self.fetch_details(to_params, 'to')

        if mode == 'deploy':
            self.deploy(from_params=from_params, to_params=to_params)

        logging.info('Looker Deployer finished.')

    def validate_user_params(self, params):
        '''
        Validating user inputs
        different functions will have different validation
        '''

        # 1 - Ensure inputs are not empty
        if params == {} or not params:
            logging.error('Configuration is missing.')
            sys.exit(1)

        # validations specifcally for fetch_details mode
        if params['mode'] == 'deploy':

            # 2 - Ensure FROM credentials are entered
            from_params = params.get(KEY_FROM)
            if from_params[KEY_BASE_URL] == '' or not from_params[KEY_CLIENT_ID] or not from_params[KEY_CLIENT_SECRET]:
                logging.error('[FROM] credentials are missing.')
                sys.exit(1)

            # 3 - Ensure folder id is specified
            if from_params['folder_id'] == '':
                logging.error('Please specify your [from] folder id.')
                sys.exit(1)

            # 4 - Ensure TO credentials are entered
            to_params = params.get(KEY_TO)
            if to_params[KEY_BASE_URL] == '' or not to_params[KEY_CLIENT_ID] or not to_params[KEY_CLIENT_SECRET]:
                logging.error('[TO] credentials are missing.')
                sys.exit(1)

            # 5 - check desginated type
            if to_params['type'] not in ('folders', 'dashboards', 'looks'):
                logging.error(f'Invalid [Export Type]: {to_params["type"]}')
                sys.exit(1)

            # 6 - make sure there are at least 1 TO path
            if len(to_params['value']) < 1:
                logging.error(
                    'Please specify what you want to export in [TO].')
                sys.exit(1)

            # 7 - making sure the target folder is configured in the TO environemnt
            if to_params['target_folder'] == '':
                logging.error(
                    'Please configure your [Target Folder] in your [TO] environment.')
                sys.exit(1)

            # 8 - testing connection with FROM credentials
            logging.info('Checking [FROM] credentials')
            from_url = from_params.get(KEY_BASE_URL)
            from_request_url = urllib.parse.urljoin(from_url, '/api/4.0')
            from_client_id = from_params.get(KEY_CLIENT_ID)
            from_client_secret = from_params.get(KEY_CLIENT_SECRET)
            from_token = self.authorize(
                url=from_request_url, client_id=from_client_id, client_secret=from_client_secret)

            # 9 - testing connection with TO credentials
            logging.info('Checking [TO] credentials')
            to_url = to_params.get(KEY_BASE_URL)
            to_request_url = urllib.parse.urljoin(to_url, '/api/4.0')
            to_client_id = to_params.get(KEY_CLIENT_ID)
            to_client_secret = to_params.get(KEY_CLIENT_SECRET)
            self.authorize(url=to_request_url, client_id=to_client_id,
                           client_secret=to_client_secret)

            # 10 - ensure the input folder_id is valid when mode is deploy
            mode = params.get(KEY_MODE)
            if mode == 'deploy':
                from_folders, from_folder_hierarchy = self.get_folder_details(
                    from_url, from_token)
                try:
                    from_folder_id = int(from_params.get('folder_id'))
                except Exception:
                    logging.error(f'{from_folder_id} is not a valid id.')
                    sys.exit(1)
                if str(from_folder_id) not in list(from_folder_hierarchy.keys()):
                    logging.error(
                        f'[{from_folder_id}] from [FROM] is not one of the available folder ids.')
                    sys.exit(1)

        elif params['mode'] == 'fetch_details':

            # 11 - ensure one of FROM or TO credentials are entered
            if params['from']['base_url'] == '' and params['to']['base_url'] == '':
                logging.error(
                    'Please configure either [FROM] or [TO] credentials for [fetch_details]')
                sys.exit(1)

            # 12 - check FROM credentaisl if configuerd
            from_params = params.get(KEY_FROM)
            if params['from']['base_url']:
                logging.info('Checking [FROM] credentials')
                from_url = from_params.get(KEY_BASE_URL)
                from_request_url = urllib.parse.urljoin(from_url, '/api/4.0')
                from_client_id = from_params.get(KEY_CLIENT_ID)
                from_client_secret = from_params.get(KEY_CLIENT_SECRET)
                from_token = self.authorize(
                    url=from_request_url, client_id=from_client_id, client_secret=from_client_secret)

            # 13 - check TO credentials if configured
            to_params = params.get(KEY_TO)
            if params['to']['base_url']:
                logging.info('Checking [TO] credentials')
                to_url = to_params.get(KEY_BASE_URL)
                to_request_url = urllib.parse.urljoin(to_url, '/api/4.0')
                to_client_id = to_params.get(KEY_CLIENT_ID)
                to_client_secret = to_params.get(KEY_CLIENT_SECRET)
                self.authorize(url=to_request_url, client_id=to_client_id,
                               client_secret=to_client_secret)

        else:

            logging.error(
                'Invalid mode. Please select either [deploy] or [fetch_details]')
            sys.exit(1)

    def post_request(self, url, header, body=None):
        '''
        Standard Post request
        '''

        r = requests.post(url=url, headers=header, data=body)

        return r

    def authorize(self, url, client_id, client_secret):
        '''
        Authorizing Looker account with client id and secret
        '''

        auth_url = urllib.parse.urljoin(url, '/api/4.0/login')
        auth_header = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        auth_body = 'client_id={}&client_secret={}'.format(
            client_id, client_secret)
        request_url = auth_url + '?' + auth_body

        res = self.post_request(request_url, auth_header)

        if res.status_code != 200:
            logging.error(
                "Authorization failed. Please check your credentials.")
            sys.exit(1)

        return res.json()['access_token']

    def create_looker_ini(self, from_params, to_params):

        logging.info('Creating Looker configuration...')
        with open('/data/looker.ini', 'w', newline='') as file:
            writer = csv.writer(file)

            self.write_looker_ini(
                writer_obj=writer, creds_type='from', creds_obj=from_params)

            self.write_looker_ini(
                writer_obj=writer, creds_type='to', creds_obj=to_params)

    def write_looker_ini(self, writer_obj, creds_type, creds_obj):

        writer_obj.writerow([f'[{creds_type}]'])

        for cred in KEY_CREDENTIALS:
            statement = f'{cred.replace("#", "")}={creds_obj.get(cred)}'
            writer_obj.writerow([statement])

        writer_obj.writerow(['verify_ssl=True'])
        writer_obj.writerow([])

    def construct_arg(self, arg_type, **kwargs):
        export_path = '/data/exports/'
        looker_creds = ['--ini', '/data/looker.ini']
        arg = ['ldeploy', 'content', f'{arg_type}'] + looker_creds

        if arg_type == 'export':
            env = ['--env', 'from']
            folder = ['--folders', f'{kwargs["folder_id"]}']
            # arg = f'{arg} {env} {folder} --local-target {export_path}'
            arg = arg + env + folder + ['--local-target', f'{export_path}']

        elif arg_type == 'import':
            env = ['--env', 'to']
            import_filename = kwargs['value']

            # dashboard/looks to export
            file_path = '/' + os.path.join(
                *export_path.split('/'), *import_filename.split('/'))
            # dest = [f'--{kwargs["type"]}', f'{export_path}{import_filename}']
            dest = [f'--{kwargs["type"]}', file_path]

            # target folder in the [to] environment
            dest.append('--target-folder')
            dest.append(f'{kwargs["target_folder"]}')

            arg = arg + env + dest

            if kwargs['type'] == 'folders':
                arg = arg + ['--recursive']

        return arg

    def deploy(self, from_params, to_params):

        # Create looker configuration
        self.create_looker_ini(from_params, to_params)

        # 1 - Exporting Content
        export_statement = self.construct_arg(
            arg_type='export', folder_id=from_params['folder_id'])
        logging.info(
            f'Exporting data from folder [{from_params["folder_id"]}]')

        try:
            logging.debug(f"Running export statement: {export_statement}")
            subprocess.run(export_statement, check=True)

        except Exception as err:
            logging.error(err)
            sys.exit(1)

        # with open("/data/exports/all_dashboards.json", 'w') as file:
        #     json.dump(self.all_dashboards, file)

        log = []
        # 2 - Importing Content
        for val in to_params['value']:

            # find relative path from "from" environment
            if to_params['type'] == 'dashboards':
                new_val = self.all_dashboards['from'][val]
            elif to_params['type'] == 'looks':
                new_val = self.all_looks['from'][val]
            else:
                new_val = val

            logging.info(f'Importing {to_params["type"]} - {new_val}')
            import_statement = self.construct_arg(
                arg_type='import', type=to_params['type'], value=new_val, target_folder=to_params['target_folder'])
            logging.info(f'import statement: {import_statement}')

            # Checking the path of the configured value exists

            folder_path = '/data/exports/'
            file_path = '/' + \
                        os.path.join(*folder_path.split('/'), *new_val.split('/'))
            logging.info(f'FILE_PATH: {file_path}')

            if os.path.exists(file_path):

                try:
                    subprocess.run(import_statement, check=True)
                    status = 'DEPLOYED'
                    issue = ''
                except Exception:
                    status = 'FAILED'
                    issue = 'Request failed.'

                tmp = {
                    'date': CURRENT_DATE,
                    'type': to_params['type'],
                    'value': val,
                    'status': status,
                    'issue': issue
                }

                log.append(tmp)

            else:

                logging.warning(f'[{val}] does not exist in path.')
                tmp = {
                    'date': CURRENT_DATE,
                    'type': to_params['type'],
                    'value': val,
                    'status': 'FAILED',
                    'issue': f'[{val}] does not exist in path.'
                }
                log.append(tmp)

        # Output log of the run
        log_df = pd.DataFrame(log)
        log_file_path = os.path.join(self.tables_out_path, 'log.csv')
        log_df.to_csv(log_file_path, index=False)

        log_manifest = {
            'incremental': True,
            'primary_key': ['date', 'type', 'value']
        }
        log_manifest_path = os.path.join(
            self.tables_out_path, 'log.csv.manifest')
        with open(log_manifest_path, 'w') as json_file:
            json.dump(log_manifest, json_file)

    def fetch_details(self, params, input_type):
        '''
        Fetching folder/dashboard details
        params: credentials of the environment
        type - str: FROM/TO environment, use for the output table name
        '''

        # Credentials
        url = params.get(KEY_BASE_URL)
        client_id = params.get(KEY_CLIENT_ID)
        client_secret = params.get(KEY_CLIENT_SECRET)
        token = self.authorize(url=url, client_id=client_id,
                               client_secret=client_secret)

        # folders
        out_folders, folder_hierarchy = self.get_folder_details(url, token)
        self._output(out_folders, f'{input_type}_folders.csv')

        # dashboard
        out_dashboards = self.get_dashboard_details(
            url, token, folder_hierarchy, input_type)
        self._output(out_dashboards, f'{input_type}_dashboards.csv')

        # looks
        out_looks = self.get_looks_details(
            url, token, folder_hierarchy, input_type)
        self._output(out_looks, f'{input_type}_looks.csv')

    def get_dashboard_details(self, url, token, folder_hierarchy, input_type):
        '''
        Getting all dashboard paths
        '''
        logging.info('Fetching dashboard details.')

        request_url = urllib.parse.urljoin(url, '/api/4.0/dashboards')
        request_header = {
            'Authorization': 'Bearer {}'.format(token),
            'Content-Type': 'application/json'
        }

        res = requests.get(request_url, headers=request_header)

        data_out = []
        logging.info(f'Total Dashboards - {len(res.json())}')

        for dashboard in res.json():

            # for Fetch-details
            tmp = {
                'environment': url,
                'dashboard_id': f"{dashboard['id']}",
                'title': dashboard['title'],
                'space': dashboard.get('space', {}).get('name'),
                'folder': dashboard['folder']['name'],
                'full_name': f'Dashboard_{dashboard["id"]}_{dashboard["title"]}.json'
            }

            full_path = f'{dashboard["folder"]["name"]}'
            parent_id = dashboard['folder']['parent_id']

            while parent_id:

                if parent_id not in folder_hierarchy:
                    parent_id = ''

                else:
                    full_path = f'{folder_hierarchy[parent_id]["name"]}/{full_path}'
                    parent_id = folder_hierarchy[parent_id]["parent_id"]

            tmp['full_path'] = f'{full_path}/{tmp["full_name"]}'
            tmp['ui_path'] = f'{full_path}/{tmp["title"]}'

            data_out.append(tmp)

            # for deploy endpoint
            dashboard_ui_path = tmp['ui_path']
            dashboard_actual_path = tmp['full_path']
            self.all_dashboards[input_type][dashboard_ui_path] = dashboard_actual_path

        return data_out

    def get_folder_details(self, url, token):
        '''
        Getting all folder details
        '''

        logging.info('Fetching folder details.')
        request_url = urllib.parse.urljoin(url, '/api/4.0/folders')
        request_header = {
            'Authorization': 'Bearer {}'.format(token),
            'Content-Type': 'application/json'
        }

        res = requests.get(request_url, headers=request_header)

        # as a output
        data_out = []
        # for fetching parent table purposes
        hierarchy = {}

        for folder in res.json():
            tmp = {
                'environemnt': url,
                'id': f"{folder['id']}",
                'name': folder['name'],
                # 'parent_id': f"{folder['parent_id']}"
                'parent_id': folder['parent_id']
            }
            data_out.append(tmp)

            hierarchy[folder['id']] = {
                'name': folder['name'],
                'parent_id': folder['parent_id']
            }

        # adding full_path
        data_out_v2 = []
        for folder in data_out:

            tmp = folder.copy()
            parent_id = tmp['parent_id']
            full_path = tmp['name']

            # while parent_id and parent_id != 'None':
            while parent_id:

                if parent_id not in hierarchy:
                    parent_id = ''

                else:
                    full_path = f'{hierarchy[parent_id]["name"]}/{full_path}'
                    parent_id = hierarchy[parent_id]["parent_id"]

            tmp['full_path'] = full_path
            data_out_v2.append(tmp)

        return data_out_v2, hierarchy

    def get_looks_details(self, url, token, folder_hierarchy, input_type):

        logging.info('Fetching Looks details.')
        request_url = urllib.parse.urljoin(url, '/api/4.0/looks')
        request_header = {
            'Authorization': 'Bearer {}'.format(token),
            'Content-Type': 'application/json'
        }

        res = requests.get(request_url, headers=request_header)

        data_out = []

        logging.info(f'Total Looks - {len(res.json())}')

        for look in res.json():

            tmp = {
                'url': url,
                'id': look['id'],
                'title': look['title'],
                'public': look['public'],
                'folder': look['folder']['name'],
                'folder_id': look['folder_id'],
                'full_name': f'Look_{look["id"]}_{look["title"]}.json'
            }

            full_path = f'{look["folder"]["name"]}'
            parent_id = int(look['folder']['parent_id']
                            ) if look['folder']['parent_id'] else None

            while parent_id:

                if parent_id not in folder_hierarchy:
                    parent_id = ''

                else:
                    full_path = f'{folder_hierarchy[parent_id]["name"]}/{full_path}'
                    parent_id = folder_hierarchy[parent_id]["parent_id"]

            tmp['full_path'] = f'{full_path}/{tmp["full_name"]}'
            tmp['ui_path'] = f'{full_path}/{tmp["title"]}'

            data_out.append(tmp)

            # for deploy endpoint
            look_ui_path = tmp['ui_path']
            look_actual_path = tmp['full_path']
            self.all_looks[input_type][look_ui_path] = look_actual_path

        return data_out

    def _output(self, data, filename):

        data_in = pd.DataFrame(data)
        data_in.to_csv(f'{self.tables_out_path}/{filename}', index=False)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.run()
    except Exception as exc:
        logging.exception(exc)
        exit(2)
