#!/usr/bin/env python3

"""
rm_guest_allusers_from_permissions.py  --  Remove group All users and guest from permissions
WGSA-38259, WGSA-33344

Usage:
  rm_guest_allusers_from_permissions.py [-d] [--noop]

Options:
  -d, --debug           Debug mode
  --noop                Run in noop mode

"""

import logging
import json
import os
import tableauserverclient as TSC
from configparser import ConfigParser
from docopt import docopt
from logging.handlers import TimedRotatingFileHandler

SCRIPT_HOME = os.path.dirname(os.path.realpath(__file__))
cred_file = os.path.join(SCRIPT_HOME, 'ad2tabsync.conf')
conf_file = os.path.join(SCRIPT_HOME, 'rm_guest_allusers_from_permissions.json')
logs_file = os.path.join(SCRIPT_HOME, 'rm_guest_allusers_from_permissions.log')


def _read_conf_file(filename):
    lgr = logging.getLogger('main.read_conf_file')
    lgr.debug(f'Try to read {filename}')
    with open(filename) as f:
        return json.load(f)


class TableauPermissionCleaner():
    def __init__(self, url, username, password, noop: bool = True):
        self.noop = noop
        self.lgr = logging.getLogger('main.TableauPermissionCleaner')
        if self.noop:
            self.lgr.warning('NOOP MODE')
        self.lgr.debug(f'__init__ url:{url}, username:{username}')
        self.user = username
        self.pwd = password
        self.tsc_server = TSC.Server(server_address=url)

    def _clear_site_from_group(self, ignore_tag, group_id):
        self.lgr.debug(f'Run _clear_site_from_group with ignore_tag:{ignore_tag}, group_id:{group_id}')

        # workbooks = [w for w in list(TSC.Pager(self.tsc_server.workbooks)) if ignore_tag not in w.tags]
        for wb in TSC.Pager(self.tsc_server.workbooks):
            self.lgr.debug(f'Proccesing "{wb.name}"')
            if ignore_tag in wb.tags:
                continue
            self.tsc_server.workbooks.populate_permissions(wb)
            for p in wb.permissions:
                if p.grantee.id == group_id and p.grantee.tag_name == 'group':
                    self.lgr.info(
                        f'Remove {str(p.capabilities)} permissions from wb: "{wb.name}" , project: "{wb.project_name})"')
                    if not self.noop:
                        self.tsc_server.workbooks.delete_permission(wb, p)

        datasources = [d for d in list(TSC.Pager(self.tsc_server.datasources)) if ignore_tag not in d.tags]
        for d in datasources:
            self.tsc_server.datasources.populate_permissions(d)
            for p in d.permissions:
                if p.grantee.id == group_id and p.grantee.tag_name == 'group':
                    self.lgr.info(
                        f'Remove {str(p.capabilities)} permissions from datasource: "{d.name}" , project: "{w.project_name})"')
                    if not self.noop:
                        self.tsc_server.datasources.delete_permission(w, p)

    def _clear_site_from_user(self, ignore_tag, user_id):
        self.lgr.debug(f'Run _clear_site_from_user with ignore_tag:{ignore_tag}, group_id:{user_id}')

        workbooks = [w for w in list(TSC.Pager(self.tsc_server.workbooks)) if ignore_tag not in w.tags]
        for w in workbooks:
            self.tsc_server.workbooks.populate_permissions(w)
            for p in w.permissions:
                if p.grantee.id == user_id and p.grantee.tag_name == 'user':
                    self.lgr.info(
                        f'Remove {str(p.capabilities)} permissions from wb: "{w.name}" , project: "{w.project_name})"')
                    if not self.noop:
                        self.tsc_server.workbooks.delete_permission(w, p)

        datasources = [d for d in list(TSC.Pager(self.tsc_server.datasources)) if ignore_tag not in d.tags]
        for d in datasources:
            self.tsc_server.datasources.populate_permissions(d)
            for p in d.permissions:
                if p.grantee.id == user_id and p.grantee.tag_name == 'user':
                    self.lgr.info(
                        f'Remove {str(p.capabilities)} permissions from datasource: "{d.name}" , project: "{d.project_name})"')
                    if not self.noop:
                        self.tsc_server.datasources.delete_permission(w, p)

    def _get_group_id_by_name(self, name):
        groups = [g.id for g in list(TSC.Pager(self.tsc_server.groups)) if g.name == name]
        if groups:
            return groups.pop()
        return None

    def _get_user_id_by_name(self, name):
        users = [i.id for i in list(TSC.Pager(self.tsc_server.users)) if i.name == name]
        if users:
            return users.pop()
        return None

    def start_clean_site(self, site_id, ignore_tag, clean_name, group: bool):
        self.lgr.info(f'start_clean_site site_id:{site_id}, ignore_tag:{ignore_tag} clean_name:{clean_name} ')
        tsc_auth = TSC.TableauAuth(username=self.user, password=self.pwd, site_id=site_id)
        self.tsc_server.auth.sign_in(tsc_auth)
        if group:
            group_id = self._get_group_id_by_name(clean_name)
            if group_id:
                self._clear_site_from_group(ignore_tag, group_id)
            else:
                self.lgr.warning(f'{clean_name} not found. Stop processing site: {site_id}')
        else:
            user_id = self._get_user_id_by_name(clean_name)
            if user_id:
                self._clear_site_from_user(ignore_tag, user_id)
            else:
                self.lgr.warning(f'{clean_name} not found. Stop processing site: {site_id}')


def main():
    lgr = logging.getLogger('main')
    argz = docopt(__doc__)
    if argz.get('--debug'):
        lgr.setLevel(logging.DEBUG)
    else:
        lgr.setLevel(logging.INFO)
    h1 = logging.StreamHandler()
    h1.setFormatter(logging.Formatter('%(levelname)s - %(name)s: %(message)s'))
    lgr.addHandler(h1)
    fh = TimedRotatingFileHandler(logs_file, when="W0", interval=1, backupCount=2)
    fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    lgr.addHandler(fh)
    lgr.debug('Run in debug mode')
    lgr.debug(argz)
    conf = _read_conf_file(conf_file)
    config_parcer = ConfigParser()
    config_parcer.read_file(open(cred_file))
    cred = config_parcer['Tableau']
    tbl_cleaner = TableauPermissionCleaner(url=cred['server'], username=cred['username'], password=cred['password'],
                                           noop=argz.get('--noop'))
    for k, v in conf.items():
        for site in v.get('sites'):
            tbl_cleaner.start_clean_site(site_id=site, ignore_tag=v.get('tag'), clean_name=k, group=v.get('group'))


if __name__ == '__main__':
    main()
