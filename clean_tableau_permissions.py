#!/usr/bin/env python3

import logging
import yaml
import os
import sys
import typer
import tableauserverclient as TSC
from configparser import ConfigParser
from typing import Optional
from logging.handlers import TimedRotatingFileHandler

SCRIPT_HOME = os.path.dirname(os.path.realpath(__file__))
CRED_FILE = os.path.join(SCRIPT_HOME, 'ad2tabsync.conf')
CONF_FILE = os.path.join(SCRIPT_HOME, 'clean_tableau_permissions.yaml')
LOG_FILE = os.path.join(SCRIPT_HOME, 'clean_tableau_permissions.log')


def init_logger(debug: bool = False, log_name='main', file=None):
    logger = logging.getLogger(log_name)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter('%(asctime)s %(name)s: %(message)s', '%Y-%m-%d %H:%M:%S'))
    logger.addHandler(sh)

    fh = TimedRotatingFileHandler(file, when="W0", interval=1, backupCount=2)
    fh.setFormatter(logging.Formatter('%(asctime)s %(name)s: %(message)s', '%Y-%m-%d %H:%M:%S'))
    logger.addHandler(fh)

    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)


def get_conf(file: str):
    with open(file, "r") as stream:
        return yaml.safe_load(stream)


def get_creds(file: str):
    config_parcer = ConfigParser()
    config_parcer.read_file(open(file))
    return config_parcer


class TableauPermissionCleaner:
    def __init__(self, server, username, password, noop: bool):
        self.log = logging.getLogger('main.tpc')
        self.server = TSC.Server(server_address=server,
                                 use_server_version=True)
        self.tableau_auth = TSC.TableauAuth(username=username,
                                            password=password)
        self.noop = noop

    def _get_user_id(self, username: str):
        req_option = TSC.RequestOptions()
        req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                         TSC.RequestOptions.Operator.Equals,
                                         username))
        user, _ = self.server.users.get(req_option)
        if user:
            return user[0].id

    def _get_group_id(self, groupname: str):
        req_option = TSC.RequestOptions()
        req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name,
                                         TSC.RequestOptions.Operator.Equals,
                                         groupname))
        group, _ = self.server.groups.get(req_option)
        if group:
            return group[0].id

    def start(self, conf: dict):
        self.server.auth.sign_in(auth_req=self.tableau_auth)
        req_option = TSC.RequestOptions()
        req_option.sort.add(TSC.Sort(TSC.RequestOptions.Field.CreatedAt,
                                     TSC.RequestOptions.Direction.Desc))

        for site in TSC.Pager(self.server.sites):
            if site.name not in [i['name'] for i in conf['sites']]:
                self.log.debug(f'Site "{site.name}" not in conf. Ignore.')
                continue
            self.log.info(f'Start processing site: "{site.name}".')
            self.server.auth.switch_site(site)

            self.log.info('Start proccesing projects')
            pr_items_clean = {'user': {},
                              'group': {}}

            for user in [i['projects']['users'] for i in conf['sites']][0]:
                user_id = self._get_user_id(username=user['name'])
                if user_id:
                    pr_items_clean['user'][user_id] = {'name': user['name']}
            for group in [i['projects']['groups'] for i in conf['sites']][0]:
                group_id = self._get_group_id(groupname=group['name'])
                if group_id:
                    pr_items_clean['group'][group_id] = {'name': group['name']}

            tableau_projects = list(TSC.Pager(endpoint=self.server.projects))
            tableau_projects_by_parent = [i for i in tableau_projects if i.parent_id is None]
            tableau_projects = [i for i in tableau_projects if i.parent_id is not None]
            while tableau_projects:
                for index, p in enumerate(tableau_projects):
                    if p.parent_id in [i.id for i in tableau_projects_by_parent]:
                        tableau_projects_by_parent.append(tableau_projects.pop(index))

            for pr in tableau_projects_by_parent:
                self.log.debug(f'Proccesing project "{pr.name}"')

                self.server.projects.populate_datarole_default_permissions(pr)
                for p in pr.default_datarole_permissions:
                    if p.grantee.id in pr_items_clean.get(p.grantee.tag_name):
                        self.log.info(f'Remove default_datarole_permissions {p.capabilities} from project "{pr.name}" '
                                      f'for \"{pr_items_clean.get(p.grantee.tag_name).get(p.grantee.id).get("name")}\"')
                        if not self.noop:
                            self.server.projects.delete_datarole_default_permissions(pr, p)

                self.server.projects.populate_datasource_default_permissions(pr)
                for p in pr.default_datasource_permissions:
                    if p.grantee.id in pr_items_clean.get(p.grantee.tag_name):
                        self.log.info(f'Remove default_datasource_permissions {p.capabilities} from project "{pr.name}" '
                                      f'for \"{pr_items_clean.get(p.grantee.tag_name).get(p.grantee.id).get("name")}\"')
                        if not self.noop:
                            self.server.projects.delete_datasource_default_permissions(pr, p)

                self.server.projects.populate_flow_default_permissions(pr)
                for p in pr.default_flow_permissions:
                    if p.grantee.id in pr_items_clean.get(p.grantee.tag_name):
                        self.log.info(f'Remove default_flow_permissions {p.capabilities} from project "{pr.name}" '
                                      f'for \"{pr_items_clean.get(p.grantee.tag_name).get(p.grantee.id).get("name")}\"')
                        if not self.noop:
                            self.server.projects.delete_flow_default_permissions(pr, p)

                self.server.projects.populate_lens_default_permissions(pr)
                for p in pr.default_lens_permissions:
                    if p.grantee.id in pr_items_clean.get(p.grantee.tag_name):
                        self.log.info(f'Remove default_lens_permissions {p.capabilities} from project "{pr.name}" '
                                      f'for \"{pr_items_clean.get(p.grantee.tag_name).get(p.grantee.id).get("name")}\"')
                        if not self.noop:
                            self.server.projects.delete_lens_default_permissions(pr, p)

                self.server.projects.populate_metric_default_permissions(pr)
                for p in pr.default_metric_permissions:
                    if p.grantee.id in pr_items_clean.get(p.grantee.tag_name):
                        self.log.info(f'Remove default_metric_permissions {p.capabilities} from project "{pr.name}" '
                                      f'for \"{pr_items_clean.get(p.grantee.tag_name).get(p.grantee.id).get("name")}\"')
                        if not self.noop:
                            self.server.projects.delete_metric_default_permissions(pr, p)

                self.server.projects.populate_workbook_default_permissions(pr)
                for p in pr.default_workbook_permissions:
                    if p.grantee.id in pr_items_clean.get(p.grantee.tag_name):
                        self.log.info(f'Remove default_workbook_permissions {p.capabilities} from project "{pr.name}" '
                                      f'for \"{pr_items_clean.get(p.grantee.tag_name).get(p.grantee.id).get("name")}\"')
                        if not self.noop:
                            self.server.projects.delete_workbook_default_permissions(pr, p)

                self.server.projects.populate_permissions(pr)
                for p in pr.permissions:
                    if p.grantee.id in pr_items_clean.get(p.grantee.tag_name):
                        self.log.info(f'Remove permissions {p.capabilities} from project "{pr.name}" '
                                      f'for \"{pr_items_clean.get(p.grantee.tag_name).get(p.grantee.id).get("name")}\"')
                        if not self.noop:
                            self.server.projects.delete_permission(pr, p)

            self.log.info('Start proccesing workbooks')
            clean_wb_users = {}
            clean_wb_groups = {}
            for user in [i['workbooks']['users'] for i in conf['sites']][0]:
                user_id = self._get_user_id(username=user['name'])
                if user_id:
                    clean_wb_users[user_id] = {'tag': user.get('tag'),
                                               'name': user['name']}
            for group in [i['workbooks']['groups'] for i in conf['sites']][0]:
                group_id = self._get_group_id(groupname=group['name'])
                if group_id:
                    clean_wb_groups[group_id] = {'tag': group.get('tag'),
                                                 'name': group['name']}

            for wb in TSC.Pager(endpoint=self.server.workbooks, request_opts=req_option):
                self.log.debug(f'Proccesing workbook "{wb.name}"')
                self.server.workbooks.populate_permissions(wb)
                for p in wb.permissions:
                    if p.grantee.tag_name == 'group' and p.grantee.id in clean_wb_groups:
                        if clean_wb_groups.get(p.grantee.id).get('tag') not in wb.tags:

                            self.log.info(f'Remove {p.capabilities} from wb "{wb.name}" '
                                          f'for \"{clean_wb_groups.get(p.grantee.id).get("name")}\"')
                            if not self.noop:
                                self.server.workbooks.delete_permission(wb, p)

                    if p.grantee.tag_name == 'user' and p.grantee.id in clean_wb_users:
                        if clean_wb_users.get(p.grantee.id).get('tag') not in wb.tags:
                            self.log.info(f'Remove {p.capabilities} from wb "{wb.name}" '
                                          f'for \"{clean_wb_users.get(p.grantee.id).get("name")}\"')
                            if not self.noop:
                                self.server.workbooks.delete_permission(wb, p)


app = typer.Typer(add_completion=False)

@app.command(context_settings=dict(help_option_names=["-h", "--help"]))
def main(debug: Optional[bool] = typer.Option(False, '-d', '--debug', show_default=True),
         noop: Optional[bool] = typer.Option(False, '--noop', show_default=True)):
    init_logger(debug=debug, file=LOG_FILE)
    log = logging.getLogger('main')
    log.debug('Debug mode')
    if noop:
        log.warning('NOOP mode')

    cred = get_creds(file=CRED_FILE)
    tab_creds = cred['Tableau']
    conf = get_conf(file=CONF_FILE)

    tpc = TableauPermissionCleaner(server=tab_creds['server'],
                                   username=tab_creds['username'],
                                   password=tab_creds['password'],
                                   noop=noop)

    tpc.start(conf)


if __name__ == "__main__":
    app()

