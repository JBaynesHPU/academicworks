# %%
# This is a sample Python script.
from csv import QUOTE_ALL
import logging
import ssl
import sys

import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from yaml import load, Loader
import stored_procedure as sp

if __name__ == '__main__':

    # Open Config File
    with open('config.yaml', 'r') as config_file:
        config = load(config_file, Loader=Loader)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')

    file_handler = logging.FileHandler('AW_export.log')
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    token = config['token']
    endpoint = config['endpoint']
    export = config['export_file']

    class Tls12Adaptor(HTTPAdapter):
        """"Transport adapter" that allows us to use TLSv1.2"""

        def init_poolmanager(self, connections, maxsize, block=False):
            self.poolmanager = PoolManager(
                num_pools=connections, maxsize=maxsize,
                block=block, ssl_version=ssl.PROTOCOL_TLSv1_2)

    df = sp.query_kolkata('EXEC dbo.AcademicWorks_SP2022_v3')
# %%
    df.to_csv('C:\\Users\\jbaynes\\Documents\\SSIS\\AcademicWorks\\export.csv',
              index=False, quoting=QUOTE_ALL)
# %%

    files = [('file', ('export.csv', open(export, 'rb'), 'text/csv'))]

    data = {'token': token}

    try:
        session = requests.session()
        session.mount("https://", Tls12Adaptor())
        r = requests.post(endpoint, data=data, files=files)
        logging.info(r.status_code)
    except Exception as e:
        # we will log any errors and do a hard exit
        logging.error(e)
        sys.exit(1)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
