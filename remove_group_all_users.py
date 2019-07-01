#!/usr/bin/env python3

#WGSA-33344: Remove group "All users from project permission"

"""
remove_group_all_users.py  Remove group "All users from projects permission.

Usage:
  remove_group_all_users.py [-d] [--noop]

Options:
  -d         Debug mode
  --noop     Run in noop mode

"""

import logging
import sys
import tableauserverclient as TSC
import xml.etree.ElementTree as etree
from configparser import ConfigParser
from docopt import docopt
from logging.handlers import TimedRotatingFileHandler

sites_for_processing = ['dev']
ignore_tag = 'all_users_report'
conf_file = 'ad2tabsync.conf'

def get_logger(name, filename=None):
    l = logging.getLogger(name)
    sh = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    sh.setFormatter(formatter)
    l.addHandler(sh)
    if filename:
        fh = TimedRotatingFileHandler(filename, when="W0", interval=1, backupCount=2)
        fh.setFormatter(formatter)
        l.addHandler(fh)
    return l

def main():
    l = get_logger(name='remove_group_all_users', filename="remove_group_all_users.log")
    if '-d' in sys.argv or '--dd' in sys.argv:
        print(f"argv: {sys.argv}")
        l.setLevel(logging.DEBUG)
    else:
        l.setLevel(logging.INFO)
    argz = docopt(__doc__, argv=sys.argv[1:])
    l.debug(f"argz: {argz}")
    if argz.get('--noop'):
        l.warning("NOOP MODE")
    config = ConfigParser()
    config.read_file(open(conf_file))

    tab = TSC.Server(server_address=config['Tableau']['server'])
    l.debug(f"Signing in to {config['Tableau']['server']}")

    for site in sites_for_processing:
        l.debug(f"site : {site}")
        tableau_auth = TSC.TableauAuth(username=config['Tableau']['username'], password=config['Tableau']['password'], site_id=site)
        tab.auth.sign_in(tableau_auth)
        all_users_group_id = [g for g in list(TSC.Pager(tab.groups)) if g.name == "All Users"].pop().id
        namespaces = {'t': 'http://tableau.com/api'}
        workbooks = [w for w in list(TSC.Pager(tab.workbooks)) if 'all_users_report' not in w.tags]
        l.debug(f"Ignore {', '.join([w.name for w in list(TSC.Pager(tab.workbooks)) if ignore_tag in w.tags])}")
        for w in workbooks:
            resp = tab.workbooks.get_request(url=f'{tab.workbooks.baseurl}/{w.id}/permissions')
            root = etree.fromstring(resp.text)
            for grantee_capabilitie in root.findall(".//t:granteeCapabilities", namespaces=namespaces):
                if grantee_capabilitie.findall('t:group',namespaces=namespaces) and grantee_capabilitie.findall('t:group',namespaces=namespaces).pop().attrib.get('id') == all_users_group_id:
                    l.debug(f"Found All users in the \"{w.name}\" permissions")
                    for capability in grantee_capabilitie.findall('.//t:capability', namespaces=namespaces):
                        url = f"{tab.workbooks.baseurl}/{w.id}/permissions/groups/{all_users_group_id}/{capability.attrib.get('name')}/{capability.attrib.get('mode')}"
                        if not argz.get('--noop'):
                            l.debug(f"url: {url}")
                            tab.groups.delete_request(url)
                        else:
                            l.warning(f'NOOP: {url}')

    l.debug('End')

if __name__ == '__main__':
    main()
