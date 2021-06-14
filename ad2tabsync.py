#!/usr/bin/env python3

"""
ad_sync is a tool for synchronizing AD users and groups with Tableau Server

Usage:
  ad2tabsync.py [-d] [--dd] [--noop] [-s SITE]
  ad2tabsync.py zabtest [-d] [--dd]
  ad2tabsync.py oldsatest [-d] [--dd] [--noop]


Options:
  -d         Debug mode
  -s SITE    Sync only the SITE site.
  --noop     Dry-run mode
  --dd       Enable debug mode for all modules with logging

"""

import logging
import ldap3
import sys
import os
import time
import random
import string
import re
import pickle
from datetime import datetime

from pyzabbix import ZabbixMetric, ZabbixSender
from logging.handlers import TimedRotatingFileHandler
from configparser import ConfigParser
from docopt import docopt
import tableauserverclient as TSC
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTP

SCRIPT_NAME = 'ad2tabsync'
SCRIPT_HOME = os.path.dirname(os.path.realpath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_HOME, SCRIPT_NAME + '.conf')


class Settings(object):
    def __init__(self, file, log_level=logging.INFO):
        self.logger = logging.getLogger(f'{SCRIPT_NAME}.Settings')
        self.logger.setLevel(log_level)
        self.config_file = file
        config = ConfigParser()
        self.logger.debug(f"Reading settings from {self.config_file}")
        config.read_file(open(self.config_file))
        self.logger.debug(f"Add AD section")
        self.settings = {'ad': {i: v for i, v in config['AD'].items()}}
        self.settings.update({'tableau': {i: v for i, v in config['Tableau'].items()}})
        if 'Zabbix' in config.sections():
            self.logger.debug(f"Found Zabbix section")
            self.settings.update({'zabbix': {i: v for i, v in config['Zabbix'].items()}})

        if 'Mail' in config.sections():
            self.logger.debug(f"Found Mail section")
            self.settings.update({'mail': {i: v for i, v in config['Mail'].items()}})


class SendMail(object):
    def __init__(self, send_to, log_level=logging.INFO, noop=False, url=''):
        self.logger = logging.getLogger(f'{SCRIPT_NAME}.SendMail')
        self.logger.setLevel(log_level)
        self.send_to = send_to
        self.noop = not noop
        self.logger.debug(f"Set self.send_to to {self.send_to}")
        self.mail_from = SCRIPT_NAME + '@' + os.uname()[1]
        self.url = url
        self.logger.debug(f"self.url: {self.url}")
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.sendmail_pickle = os.path.join(script_dir, 'SendMail.pickle')
        if os.path.isfile(self.sendmail_pickle):
            with open(self.sendmail_pickle, 'rb') as f:
                self.sent_state = pickle.load(f)
        else:
            self.sent_state = {}

    def send_mail_old_serveradministrator(self, name):
        current_date = datetime.now()
        mail_text = f"The ad2tabsync script on {self.url} has found old server administrator and can not remove his. \n The old server administrator: {name}"
        mail_subj = f"The ad2tabsync script on {self.url} has found old server administrator"
        if self.sent_state.get(name):
            if (current_date - self.sent_state.get(name)).days > 3:
                self.send_mail(subj=mail_subj, text=mail_text)
                self.sent_state.update({name: current_date})
            else:
                self.logger.debug(f"The script has already sent a letter about  oldserveradmin:{name}")
        else:
            self.send_mail(subj=mail_subj, text=mail_text)
            self.sent_state.update({name: current_date})

        with open(self.sendmail_pickle, 'wb') as f:
            pickle.dump(self.sent_state, f)

    def send_mail(self, text, subj='AD to Tableau server synchronization error.'):
        self.logger.debug(f'Send mail. mail_to: {self.send_to}, mail_from: {self.mail_from}')
        msg = MIMEMultipart()

        msg['From'] = self.mail_from
        msg['To'] = self.send_to
        msg['Subject'] = subj
        body = text
        msg.attach(MIMEText(body, 'plain'))
        server = SMTP()
        text = msg.as_string()

        if isinstance(self.send_to, str):
            self.send_to = [m.strip() for m in self.send_to.split(',')]
        if self.noop:
            try:
                server.connect()
            except Exception as e:
                self.logger.error(e)
                return
            try:
                server.sendmail(self.mail_from, self.send_to, text)
            except Exception as e:
                self.logger.error(e)
                return
            finally:
                server.quit()
        else:
            self.logger.info("NOOP is True. Mail didn't send")
            self.logger.info(f"subj: {subj}")
            self.logger.info(f"text: {text}")

class AD:
    def __init__(self, server, user, password, tableau_root_ou, users_root_ou):
        self.logger = logging.getLogger(f'{SCRIPT_NAME}.AD')
        self.tableau_root_ou = tableau_root_ou
        self.users_root_ou = users_root_ou
        try:
            self.logger.debug(f"Try connect to {server}")
            server = ldap3.Server(server, use_ssl=True)
            self.conn_async = ldap3.Connection(server, user=user, password=password, raise_exceptions=True,
                                               client_strategy=ldap3.ASYNC)
            self.conn = ldap3.Connection(server, user=user, password=password, raise_exceptions=True)
            self.conn.bind()
            self.conn_async.bind()
            self.logger.debug("A connection was successfully established with the {0}".format(server))
        except Exception as e:
            self.logger.info("Failed to connect to {0}".format(server))
            self.logger.debug(e)
            sys.exit()

    def _search(self, search_base, search_filter='(objectClass=*)', search_scope=ldap3.BASE, attributes=ldap3.ALL_ATTRIBUTES):
        try:
            self.conn.search(search_base=search_base,
                             search_filter=search_filter,
                             search_scope=search_scope,
                             attributes=attributes)
        except Exception as e:
            self.logger.exception("The query not properly ended")
            sys.exit()
        return self.conn.entries

    def _get_group_members(self, dn, group_list=[]):
        users = []
        result = self._search(dn, '(objectClass=*)')
        self.logger.debug("Get users form {0}".format(result[0].name.value))
        if ' member: ' in str(result):
            for member in result[0].member:
                ad_object = self._get_object_data(member)
                if ad_object.objectCategory.value.startswith('CN=Person'):
                    if not any(user.sAMAccountName.value == ad_object.sAMAccountName.value for user in
                               users) and self._is_user_enabled(ad_object.distinguishedName.value):
                        users.append(ad_object)
                if ad_object.objectCategory.value.startswith('CN=Group') and not any(
                                ad_object.distinguishedName.value == group for group in group_list):
                    group_list.append(ad_object.distinguishedName.value)
                    [users.append(newuser) for newuser in
                     self._get_group_members(ad_object.distinguishedName.value, group_list) if
                     not any(user.sAMAccountName.value == newuser.sAMAccountName.value for user in users)]
        return users

    def _is_user_enabled(self, dn):
        current_time_stamp = int(time.time()) * 10000000 + 116444736000000000
        response = self._search(dn, '(&(|(accountExpires=0)(accountExpires>={0}))(!(userAccountControl:1.2.840.113556.1.4.803:=2)))'.format(current_time_stamp), ldap3.BASE, ['distinguishedName'])
        if response.__len__() != 0:
            return True
        return False

    def _get_object_data(self, dn):
        result = self._search(dn, '(objectClass=*)', ldap3.BASE,
                              ['name', 'distinguishedName', 'mail', 'samaccountname', 'objectcategory',
                               'accountExpires', 'enabled', 'objectClass'])
        if result:
            return result[0]
        else:
            return result

    def get_tableau_ous(self):
        resp = self._search(self.tableau_root_ou, '(objectClass=organizationalUnit)', ldap3.LEVEL, ['name', 'distinguishedName'])
        return [i.name.value for i in resp]

    def get_all_site_users(self, sitename):
        self.logger.debug("Get all users on site {0}".format(sitename))
        site_users = []
        groups = self.get_site_groups(sitename)
        for group in groups:
            members = self._get_group_members(group.distinguishedName.value)
            [site_users.append(newuser) for newuser in members if
             not any(user.sAMAccountName.value == newuser.sAMAccountName.value for user in site_users)]
        return site_users

    def get_user_by_samaccountname(self, samaccountname):
        result = self._search(self.users_root_ou,
                              '(&(objectCategory=person)(objectClass=user)(sAMAccountName={0}))'.format(samaccountname),
                              ldap3.SUBTREE, ['name', 'distinguishedName', 'mail', 'samaccountname', 'objectcategory',
                                        'accountExpires', 'enabled', 'objectClass'])
        return result

    def get_group_by_samaccountname(self, samaccountname):
        samaccountname_escaped = ldap3.utils.conv.escape_filter_chars(samaccountname)
        result = self._search(self.tableau_root_ou, '(Name={0})'.format(samaccountname_escaped), ldap3.SUBTREE,
                              ['distinguishedName', 'name', 'member'])
        return result

    def get_site_groups(self, sitename):
        group_search_base = "OU={0},{1}".format(sitename, self.tableau_root_ou)
        groups = self._search(group_search_base, '(objectClass=Group)', ldap3.LEVEL, ['name', 'distinguishedName'])
        return groups

    def get_members_by_groupname(self, groupname):
        #self.logger.debug("Get users form {0}".format(groupname))
        group = self.get_group_by_samaccountname(groupname)
        members = self._get_group_members(group[0].distinguishedName.value)
        return members


class AD2TabSync(object):
    def __init__(self, settings, log_level, noop=False):
        self.settings = settings
        self.noop = not noop
        self.logger = logging.getLogger(f'{SCRIPT_NAME}.AD2TabSync')
        self.logger.setLevel(log_level)
        self.mails = SendMail(send_to=self.settings.get('mail').get('send_to'), log_level=log_level, url=self.settings.get('tableau').get('server'))
        self.ad = AD(server=self.settings.get('ad').get('server'),
                user=self.settings.get('ad').get('user'),
                password=self.settings.get('ad').get('password'),
                tableau_root_ou=self.settings.get('ad').get('tableau_root_ou'),
                users_root_ou=self.settings.get('ad').get('users_root_ou'))
        self.serviceaccounts = set(self.settings.get('tableau').get('serviceaccounts').split(','))
        self.tableau_auth = TSC.TableauAuth(self.settings.get('tableau').get('username'),
                                       self.settings.get('tableau').get('password'))
        self.tab = TSC.Server(self.settings.get('tableau').get('server'))
        self.logger.debug(f"Signing in to {self.settings.get('tableau').get('server')}")
        self.tab.auth.sign_in(self.tableau_auth)


    def _sync_site_user(self):
        self.logger.debug('Revision users on site')
        tableau_all_site_users = [user for user in TSC.Pager(self.tab.users)]
        # tableau_unlicensed_users = [user for user in TSC.Pager(tab.users) if user.site_role == 'ServerAdministrator']
        # self.logger.debug(f"tableau_unlicensed_users: {', '.join([u.name for u in  tableau_unlicensed_users])}")
        self.logger.debug('Revision users on AD')
        ad_all_site_users = self.ad.get_all_site_users(self.site_name)
        old_users = list(set([u.name for u in tableau_all_site_users]) - set(
            [u.sAMAccountName.value for u in ad_all_site_users]) - self.serviceaccounts)
        new_users = list(set([u.sAMAccountName.value for u in ad_all_site_users]) - set(
            [u.name for u in tableau_all_site_users]) - self.serviceaccounts)

        # This ugly code, but I was forced to write this.
        if self.site_name == 'ERS':
            self.logger.info(f"Site ERS. Set old_users to None")
            old_users = []
        # End ugly code

        if old_users:
            self.logger.info(f"Old users: {old_users}")
        if new_users:
            self.logger.info(f"New users: {new_users}")

        for user in old_users:
            self.logger.info(f"Removing {user}")
            user_obj = [u for u in tableau_all_site_users if u.name == user].pop()
            if user_obj.site_role == 'ServerAdministrator':
                self.logger.info(f"{user_obj.name} is ServerAdministrator. Send mail")
                if self.noop:
                    self.mails.send_mail_old_serveradministrator(name=user_obj.name)
            else:
                self.tab.users.populate_workbooks(user_obj)
                user_workbooks = [b for b in user_obj.workbooks if b.owner_id == user_obj.id]
                if len(user_workbooks) > 0:
                    self.logger.info(f"{user_obj.name} has workbooks: {[w.name for w in user_workbooks]}")
                    if not user_obj.site_role == 'Unlicensed':
                        old_role = user_obj.site_role
                        user_obj.site_role = 'Unlicensed'
                        if self.noop:
                            self.tab.users.update(user_obj)
                        self.logger.info(f"{user_obj.name}'s user role has been changed from {old_role} to Unlicensed.")
                    else:
                        self.logger.info(f"{user_obj.name} already has role Unlicensed. Skip")
                else:
                    if self.noop:
                        try:
                            self.tab.users.remove(user_obj.id)
                        except TSC.server.endpoint.exceptions.ServerResponseError as ServerResponseError:
                            if not ServerResponseError.code == '409003':
                                raise ServerResponseError
                    self.logger.info(f"{user} has been removed")

        for user in new_users:
            if user in [u.name for u in tableau_all_site_users if u.site_role != 'Unlicensed']:
                tableau_user = [u for u in tableau_all_site_users if u.name == user].pop()
                self.logger.info('Change site role to Interactor for {0}'.format(user))
                tableau_user.site_role = 'Interactor'
                if self.noop:
                    self.tab.users.update(tableau_user)

            else:
                self.logger.info("Creating user: {0}".format(user))
                ad_user_data = self.ad.get_user_by_samaccountname(user)
                password = ''.join(random.choice(string.ascii_letters + string.digits) for x in range(32))
                new_user = TSC.UserItem(name=ad_user_data[0].sAMAccountName.value, site_role='Interactor')
                if self.noop:
                    new_user = self.tab.users.add(new_user)
                new_user.email = ad_user_data[0].mail.value
                new_user.fullname = ad_user_data[0].name.value
                new_user.password = password
                if self.noop:
                    _ = self.tab.users.update(new_user)
        self.logger.debug("Start revision userdata on the site")

        tableau_site_users = [u for u in TSC.Pager(self.tab.users) if u.site_role != 'Unlicensed' and u.name not in self.serviceaccounts]
        for user in tableau_site_users:
            #It's tableau server bug.
            #Tableau returns users tho was deleted in the previous step.
            try:
                tuser_obj = self.tab.users.get_by_id(user.id)
            except Exception as e:
                self.logger.warning(f"Error while self.tab.users.get_by_id{e}")
            else:
                auser_obj = self.ad.get_user_by_samaccountname(tuser_obj.name)
                if auser_obj:
                    if tuser_obj.fullname != auser_obj[0].name.value:
                        self.logger.info(
                            f"Changing {tuser_obj.name}'s fullname: {tuser_obj.fullname} -> {auser_obj[0].name.value}")
                        tuser_obj.fullname = auser_obj[0].name.value
                        if self.noop:
                            self.tab.users.update(tuser_obj)
                            tuser_obj.email = auser_obj[0].mail.value

    def _sync_site_groups(self):
        self.logger.debug('Revision groups on site')
        ad_site_groups = [g.name.value for g in self.ad.get_site_groups(self.site_name)]
        tableau_site_groups = [g.name for g in TSC.Pager(self.tab.groups)]
        new_groups = set(ad_site_groups) - set(tableau_site_groups)
        old_groups = set(tableau_site_groups) - set(ad_site_groups)
        old_groups.remove('All Users')
        # This ugly code, but I was forced to write this.
        if self.site_name == 'ERS':
            self.logger.info(f"Site ERS. Remove from old_groups F_* and A_*")
            old_groups = [t for t in old_groups if not (t.startswith('F_') or t.startswith('A_'))]
        # End ugly code

        if new_groups:
            self.logger.info("New groups {0}".format(new_groups))
        if old_groups:
            self.logger.info("Old groups {0}".format(old_groups))
        for group in new_groups:
            new_group = TSC.GroupItem(group)
            self.logger.info(f"Creating group {group}")
            if self.noop:
                self.tab.groups.create(new_group)
        for group in old_groups:
            group_id = [g.id for g in TSC.Pager(self.tab.groups) if g.name == group].pop()
            self.logger.info(f"Removing group {group}")
            if self.noop:
                self.tab.groups.delete(group_id)

    def _sync_site_memberships(self):
        self.logger.debug('Revision group members on site')
        tableau_groups = [g for g in TSC.Pager(self.tab.groups)]
        opts = TSC.RequestOptions(pagesize=1000)

        # This ugly code, but I was forced to write this.
        if self.site_name == 'ERS':
            self.logger.info("Site ERS. Remove from tableau_groups F_* and A_*")
            tableau_groups = [t for t in tableau_groups if not (t.name.startswith('F_') or t.name.startswith('A_'))]
        # End ugly code

        tableau_site_users = [u for u in TSC.Pager(self.tab.users)]

        for group in [g for g in tableau_groups if g.name != 'All Users']:
            self.tab.groups.populate_users(group, opts)
            tableau_members_set = set([user.name for user in group.users])
            ad_members = self.ad.get_members_by_groupname(group.name)
            ad_members_set = set([u.sAMAccountName.value for u in ad_members])
            new_members_set = ad_members_set - tableau_members_set
            old_members_set = tableau_members_set - ad_members_set
            if new_members_set:
                self.logger.info("{0}, new members:{1} ".format(group.name, new_members_set))
            if old_members_set:
                self.logger.info("{0}, old members{1}".format(group.name, old_members_set))
            for new_member in new_members_set:
                self.logger.info(f"Adding {new_member} to {group.name}")
                user_id = [user.id for user in tableau_site_users if user.name == new_member]
                if user_id:
                    if self.noop:
                        self.tab.groups.add_user(user_id=user_id.pop(), group_item=group)
                else:
                    self.logger.warning(f"Can't add {new_member} to {group.name}. User not found")
            for old_member in old_members_set:
                self.logger.info(f"Removing {old_member} to {group.name}")
                user_id = [user.id for user in tableau_site_users if user.name == old_member].pop()
                if self.noop:
                    self.tab.groups.remove_user(user_id=user_id, group_item=group)

    def _sync_site(self, site):
        self.site_name = site.name
        self.logger.info(f"Start sync site {self.site_name}, content_url: {site.content_url}")
        self.tableau_auth.site_id = site.content_url
        self.tab.auth.sign_in(self.tableau_auth)
        self._sync_site_user()
        self._sync_site_groups()
        self._sync_site_memberships()

    def run_sync(self, site_id):
        return_code = 0
        ad_sites = self.ad.get_tableau_ous()
        tab_sites = [site for site in TSC.Pager(self.tab.sites)]
        self.logger.debug(f"Tab sites: {', '.join(sorted([s.name for s in tab_sites]))}")
        self.logger.debug(f"AD sites: {', '.join(sorted(ad_sites))}")
        common_sites = [s for s in tab_sites if s.name in ad_sites]
        if site_id:
            common_sites = [s for s in common_sites if s.content_url == site_id]
        self.logger.info(f"Found sites: {', '.join(sorted([s.name for s in common_sites]))}")

        for s in common_sites:
            try:
                self._sync_site(site=s)
            except Exception as e:
                self.logger.exception(f"Error while {s.content_url} sync")

                return_code = 1
        return return_code


class Zabbix_send(object):
    def __init__(self, config_file):
        self.logger = logging.getLogger(f'{SCRIPT_NAME}.Zabbix_send')
        zabbix_config = open(config_file).read()
        self.server = re.search(r'ServerActive=(.+)', zabbix_config).group(1)
        self.logger.debug(f"self.server: {self.server}")
        self.hostname = re.search(r'Hostname=(.+)', zabbix_config).group(1)
        self.logger.debug(f"self.hostname: {self.hostname}")

    def send(self, item, value):
        packet = [ZabbixMetric(self.hostname, item, value)]
        self.logger.debug(f"Send {packet} to {self.server}")
        return ZabbixSender(zabbix_server=self.server).send(packet)


def main():
    exit_code = 0
    if '-d' in sys.argv or '--dd' in sys.argv:
        print(f"argv: {sys.argv}")
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    argz = docopt(__doc__, argv=sys.argv[1:])

    if argz.get('--dd'):
        main_logger = logging.getLogger()
        main_logger.name = 'tableau.' + 'main'
    else:
        main_logger = logging.getLogger(f'{SCRIPT_NAME}')
    main_logger.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    main_logger.addHandler(sh)
    log_path = os.path.dirname(os.path.abspath(__file__))
    fh = TimedRotatingFileHandler(os.path.join(log_path, SCRIPT_NAME + '.log'), when="W0", interval=1, backupCount=1)
    fh.setFormatter(formatter)
    main_logger.addHandler(fh)
    main_logger.info('Starting')
    if argz.get('--noop'):
        main_logger.warning("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Dry run!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    try:
        s = Settings(file=CONFIG_PATH, log_level=log_level)
    except Exception as e:
        main_logger.exception("Error while read conf file")
        sys.exit(1)
    if s.settings.get('zabbix'):
        z = Zabbix_send(s.settings.get('zabbix').get('zabbix_conf'))
        item_name = s.settings.get('zabbix').get('item')
    if argz.get('zabtest'):
        print(z.send(item_name, 1))
        sys.exit(0)

    try:
        mails = SendMail(send_to=s.settings.get('mail').get('send_to'), log_level=log_level, noop=argz.get('--noop'))
    except Exception as e:
        exit_code = 1
        if s.settings.get('zabbix'):
            _ = z.send(item_name, exit_code)

    if argz.get('oldsatest'):
        main_logger.info("Test send_mail_old_serveradministrator function")
        mails.send_mail_old_serveradministrator('s_korneev')
        sys.exit(0)

    try:
        ad2tabsync = AD2TabSync(settings=s.settings, log_level=log_level, noop=argz.get('--noop'))
    except Exception as e:
        #mails.send_mail(text=e, subj='Error in AD2TabSync.__init__')
        main_logger.exception("Error while AD2TabSync")
        exit_code = 1
        if s.settings.get('zabbix'):
            _ = z.send(item_name, exit_code)

    return_code = ad2tabsync.run_sync(argz.get('-s'))
    main_logger.debug(f"return_code: {return_code}")
    if return_code != 0:
        exit_code = return_code
    if s.settings.get('zabbix'):
        _ = z.send(item_name, exit_code)
        main_logger.debug(f"Sent {exit_code} to zabbix")
    main_logger.info(f"Exit with code:{exit_code}")
    sys.exit(exit_code)

if __name__ == '__main__':
    main()
