'''
Template Component main class.

'''
import logging
import os
from pathlib import Path
import csv
import sys

from keboola.component import CommonInterface

# configuration variables

# #### Keep for debug
KEY_DEBUG = 'debug'
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

APP_VERSION = '0.0.1'


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

        # FROM Parameters
        from_params = params.get(KEY_FROM)

        # TO Parameters
        to_params = params.get(KEY_TO)

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
            logging.debug(f'{import_statement}')

            try:
                os.system(import_statement)
            except Exception as err:
                logging.error(err)
                sys.exit(1)

        logging.info('Looker Deployer finished.')

    def create_looker_ini(self, from_params, to_params):

        logging.info('Creating Looker configuration...')
        with open('/code/data/looker.ini', 'w', newline='') as file:
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
        export_path = '/code/data/exports/'
        looker_creds = '--ini /code/data/looker.ini'
        arg = f'ldeploy content {arg_type} {looker_creds}'

        if arg_type == 'export':
            env = '--env from'
            folder = f'--folders {kwargs["folder_id"]}'
            arg = f'{arg} {env} {folder} --local-target {export_path}'

        elif arg_type == 'import':
            env = '--env to'
            dest = f'--{kwargs["type"]} {export_path}{kwargs["value"]}'
            arg = f'{arg} {env} {dest}'

            if kwargs['type'] == 'folders':
                arg = arg + ' --recursive'

        return arg


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
