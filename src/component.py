'''
Template Component main class.

'''
import logging
import os
import subprocess
from pathlib import Path
import csv
import sys
import requests
from urllib.request import pathname2url  # noqa
import urllib.parse
import pandas as pd

from keboola.component import CommonInterface


sys.tracebacklimit = 0

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

APP_VERSION = '0.0.2'


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
        data_folder_path = get_data_folder_path()
        super().__init__(data_folder_path=data_folder_path)

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

        if mode == 'deploy':
            self.deploy(from_params=from_params, to_params=to_params)

        elif mode == 'fetch_details':
            # Details for FROM
            self.fetch_details(from_params, 'from')
            # Details for TO
            self.fetch_details(to_params, 'to')

        logging.info('Looker Deployer finished.')

    def validate_user_params(self, params):
        '''
        Validating user inputs
        '''

        # 1 - Ensure inputs are not empty
        if params == {} or not params:
            logging.error('Configuration is missing.')
            sys.exit(1)

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
            logging.error('Please specify what you want to export in [TO].')
            sys.exit(1)

        # 7 - testing connection with FROM credentials
        logging.info('Checking [FROM] credentials')
        from_url = from_params.get(KEY_BASE_URL)
        from_request_url = urllib.parse.urljoin(from_url, '/api/3.1')
        from_client_id = from_params.get(KEY_CLIENT_ID)
        from_client_secret = from_params.get(KEY_CLIENT_SECRET)
        from_token = self.authorize(
            url=from_request_url, client_id=from_client_id, client_secret=from_client_secret)

        # 8 - testing connection with TO credentials
        logging.info('Checking [TO] credentials')
        to_url = to_params.get(KEY_BASE_URL)
        to_request_url = urllib.parse.urljoin(to_url, '/api/3.1')
        to_client_id = to_params.get(KEY_CLIENT_ID)
        to_client_secret = to_params.get(KEY_CLIENT_SECRET)
        self.authorize(url=to_request_url, client_id=to_client_id,
                       client_secret=to_client_secret)

        # 9 - ensure the input folder_id is valid when mode is deploy
        mode = params.get(KEY_MODE)
        if mode == 'deploy':
            from_folders, from_folder_hierarchy = self.get_folder_details(
                from_url, from_token)
            try:
                from_folder_id = int(from_params.get('folder_id'))
            except Exception:
                logging.error(f'{from_folder_id} is not a valid id.')
                sys.exit(1)
            if from_folder_id not in from_folder_hierarchy:
                logging.error(
                    f'[{from_folder_id}] from [FROM] is not one of the available folder ids.')
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

        auth_url = urllib.parse.urljoin(url, '/api/3.1/login')
        auth_header = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        auth_body = 'client_id={}&client_secret={}'.format(
            client_id, client_secret)
        request_url = auth_url+'?'+auth_body

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

            statement = f'{cred.replace("#","")}={creds_obj.get(cred)}'
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

            dest = [f'--{kwargs["type"]}', f'{export_path}{import_filename}']

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
            os.system(export_statement)
        except Exception as err:
            logging.error(err)
            sys.exit(1)

        # 2 - Importing Content
        for val in to_params['value']:
            logging.info(f'Importing {to_params["type"]} - {val}')
            import_statement = self.construct_arg(
                arg_type='import', type=to_params['type'], value=val)
            logging.info(f'{import_statement}')

            try:
                # os.system(import_statement)
                subprocess.run(import_statement, check=True)
                # subprocess.run(import_statement_split, check=True)
            except Exception as err:
                logging.error(err)
                sys.exit(1)

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
            url, token, folder_hierarchy)
        self._output(out_dashboards, f'{input_type}_dashboards.csv')

        # looks
        out_looks = self.get_looks_details(url, token)
        self._output(out_looks, f'{input_type}_looks.csv')

    def get_dashboard_details(self, url, token, folder_hierarchy):
        '''
        Getting all dashboard paths
        '''

        request_url = urllib.parse.urljoin(url, '/api/3.1/dashboards')
        request_header = {
            'Authorization': 'Bearer {}'.format(token),
            'Content-Type': 'application/json'
        }

        res = requests.get(request_url, headers=request_header)

        data_out = []
        for dashboard in res.json():

            tmp = {
                'dashboard_id': f"{dashboard['id']}",
                'title': dashboard['title'],
                'space': dashboard['space']['name'],
                'folder': dashboard['folder']['name'],
                'full_name': f'Dashboard_{dashboard["id"]}_{dashboard["title"]}.json'
            }

            full_path = f'{dashboard["folder"]["name"]}'
            parent_id = dashboard['folder']['parent_id']

            while parent_id:
                full_path = f'{folder_hierarchy[parent_id]["name"]}/{full_path}'
                parent_id = folder_hierarchy[parent_id]["parent_id"]

            tmp['full_path'] = f'{full_path}/{tmp["full_name"]}'

            data_out.append(tmp)

        return data_out

    def get_folder_details(self, url, token):
        '''
        Getting all folder details
        '''

        request_url = urllib.parse.urljoin(url, '/api/3.1/folders')
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
                'id': f"{folder['id']}",
                'name': folder['name'],
                'parent_id': f"{folder['parent_id']}"
            }
            data_out.append(tmp)

            hierarchy[folder['id']] = {
                'name': folder['name'],
                'parent_id': folder['parent_id']
            }

        return data_out, hierarchy

    def get_looks_details(self, url, token):

        request_url = urllib.parse.urljoin(url, '/api/4.0/looks')
        request_header = {
            'Authorization': 'Bearer {}'.format(token),
            'Content-Type': 'application/json'
        }

        res = requests.get(request_url, headers=request_header)

        data_out = []

        for look in res.json():
            tmp = {
                'id': look['id'],
                'title': look['title'],
                'public': look['public'],
                'folder': look['folder']['name'],
                'folder_id': look['folder_id']
            }
            data_out.append(tmp)

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
