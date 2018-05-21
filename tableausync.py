#!/usr/bin/env python

import sys, os, time, random, string
import configparser
import logging
from logging.handlers import TimedRotatingFileHandler
import random
import string
import re
import tableauserverclient as TSC
import ldap3
from ldap3 import SUBTREE, LEVEL, ALL_ATTRIBUTES, BASE
from ZabbixSender import ZabbixSender, ZabbixPacket


log_path = os.path.dirname(os.path.abspath(__file__))

logger = logging.getLogger('tableau_sync')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

fh = TimedRotatingFileHandler(os.path.join(log_path, 'tableausync.log'), when="H", interval=1, backupCount=5)
fh.setFormatter(formatter)
sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(sh)

tbl_logger = logging.getLogger('tableau')
tbl_logger.setLevel(logging.DEBUG)
file_handler = TimedRotatingFileHandler(os.path.join(log_path, 'tableausync.debug'), when="D", interval=1,
                                        backupCount=3, )
file_handler.setFormatter(formatter)
tbl_logger.addHandler(file_handler)

class AD:
    def __init__(self, ad_server, ad_user, ad_password, tableau_root_ou, users_root_ou):
        self.logger = logging.getLogger('tableau_sync.ad')
        self.tableau_root_ou = tableau_root_ou
        self.users_root_ou = users_root_ou
        try:
            server = ldap3.Server(ad_server, use_ssl=True)
            self.conn = ldap3.Connection(server, user=ad_user, password=ad_password, raise_exceptions=True)
            self.conn.bind()
            self.logger.info("A connection was successfully established with the {0}".format(server))

        except Exception as e:
            self.logger.info("Failed to connect to {0}".format(server))
            self.logger.debug(e)
            sys.exit()

    def _search(self, search_base, search_filter='(objectClass=*)', search_scope=BASE, attributes=ALL_ATTRIBUTES):
        try:
            self.conn.search(search_base=search_base,
                             search_filter=search_filter,
                             search_scope=search_scope,
                             attributes=attributes)
        except Exception as e:
            self.logger.debug("The query not properly ended")
            self.logger.debug(e.message)
            sys.exit()

        return self.conn.entries

    def get_members_by_groupname(self, groupname):
        self.logger.debug("Get users form {0}".format(groupname))
        group = self.get_group_by_samaccountname(groupname)
        members = self._get_group_members(group[0].distinguishedName.value)
        return members

    def _get_group_members(self, dn, group_list=[]):
        users = []
        result = self._search(dn, '(objectClass=*)')
        self.logger.debug("Get users form {0}".format(result[0].name.value))
        if 'member' in str(result):
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
        response = self._search(dn,
                                '(&(|(accountExpires=0)(accountExpires>={0}))(!(userAccountControl:1.2.840.113556.1.4.803:=2)))'.format(
                                    current_time_stamp), BASE, ['distinguishedName'])
        if response.__len__() != 0:
            return True
        return False

    def _get_object_data(self, dn):
        result = self._search(dn, '(objectClass=*)', BASE,
                              ['name', 'distinguishedName', 'mail', 'samaccountname', 'objectcategory',
                               'accountExpires', 'enabled', 'objectClass'])
        if result:
            return result[0]
        else:
            return result

    def get_tableau_ous(self):
        adsites = self._search(self.tableau_root_ou, '(objectClass=organizationalUnit)', LEVEL,
                               ['name', 'distinguishedName'])
        return list(x.entry_attributes_as_dict for x in adsites)

    def get_all_site_users(self, tableausite):
        self.logger.debug("Get all users on site {0}".format(tableausite.name))
        site_users = []
        groups = self.get_site_groups(tableausite)
        for group in groups:
            members = self._get_group_members(group.distinguishedName.value)
            [site_users.append(newuser) for newuser in members if
             not any(user.sAMAccountName.value == newuser.sAMAccountName.value for user in site_users)]

        return site_users

    def get_user_by_samaccountname(self, samaccountname):
        result = self._search(self.users_root_ou,
                              '(&(objectCategory=person)(objectClass=user)(sAMAccountName={0}))'.format(samaccountname),
                              SUBTREE, ['name', 'distinguishedName', 'mail', 'samaccountname', 'objectcategory',
                                        'accountExpires', 'enabled', 'objectClass'])
        return result

    def get_group_by_samaccountname(self, samaccountname):
        samaccountname_escaped = ldap3.utils.conv.escape_filter_chars(samaccountname)
        result = self._search(self.tableau_root_ou, '(Name={0})'.format(samaccountname_escaped), SUBTREE,
                              ['distinguishedName', 'name', 'member'])
        return result

    def get_site_groups(self, tableausite):
        group_search_base = "OU={0},{1}".format(tableausite.name, self.tableau_root_ou)
        groups = self._search(group_search_base, '(objectClass=Group)', LEVEL, ['name', 'distinguishedName'])
        return groups


def main():
    logger.debug('Loading config...')
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'tableausync.conf')
    config.read_file(open(config_path))

    config_sections = config.sections()

    do_something = config.get('Common', 'do_something')
    if do_something in ['True', 'true', '1', 't', 'y', 'yes', 'yeah', 'yup', 'certainly', 'uh-huh']:
        do_something = True
    else:
        do_something = False
        logger.info('It is TEST RUN')

    ad_server = config.get('AD', 'server')
    ad_user = config.get('AD', 'user')
    ad_password = config.get('AD', 'password')
    tableau_root_ou = config.get('AD', 'tableau_root_ou')
    users_root_ou = config.get('AD', 'users_root_ou')
    tableau_server = config.get('Tableau', 'server')
    tableau_admin = config.get('Tableau', 'username')
    tableau_password = config.get('Tableau', 'password')
    tableau_service_accounts = config.get('Tableau', 'serviceAccounts').split(',')



    ad = AD(ad_server, ad_user, ad_password, tableau_root_ou, users_root_ou)

    tableau_auth = TSC.TableauAuth(tableau_admin, tableau_password)
    tableau = TSC.Server(tableau_server)

    try:
        tableau.auth.sign_in(tableau_auth)
    except Exception as e:
        logger.debug("Failed to connect to {0}".format(tableau_server))
        logger.debug(e.message)
        sys.exit()

    ad_ous = ad.get_tableau_ous()
    tableau_sites = [site for site in TSC.Pager(tableau.sites)]
    logger.info("Tableau sites: {0}".format([s.name for s in tableau_sites]))
    logger.info("AD OUs: {0}".format([ou.get('name') for ou in ad_ous]))

    #tableau_sites = [t for t in tableau_sites if t.name == 'ERS']

    for current_site in tableau_sites:
        if any(current_site.name in ad_ou.get('name') for ad_ou in ad_ous):
            print('\n')
            logger.info("Tableau site: {0} ".format(current_site.name))
            tableau_auth = TSC.TableauAuth(tableau_admin, tableau_password, current_site.content_url)
            tableau.auth.sign_in(tableau_auth)
            tableau_all_site_users = [user for user in TSC.Pager(tableau.users) if user.site_role != 'Unlicensed']
            tableau_unlicensed_users = [user for user in TSC.Pager(tableau.users) if user.site_role == 'Unlicensed']

            logger.info('Revision users on site')
            ad_all_site_users = ad.get_all_site_users(current_site)
            tableau_all_site_users_set = set([user.name for user in tableau_all_site_users])
            ad_all_site_users_set = set([user.sAMAccountName.value for user in ad_all_site_users])
            old_users_set = tableau_all_site_users_set - ad_all_site_users_set
            new_users_set = ad_all_site_users_set - tableau_all_site_users_set
            old_users_set -= set(tableau_service_accounts)
            old_users = [tableau_all_site_user for tableau_all_site_user in tableau_all_site_users if
                         any(olduser == tableau_all_site_user.name for olduser in old_users_set)]

            # This ugly code, but I was forced to write this.
            if current_site.name == 'ERS':
                old_users = []
            # End ugly code

            logger.info("Old users: {0}".format([u.name for u in old_users]))
            if do_something:
                for old_user in old_users:
                    logger.debug("Deleting user: {0}".format(old_user.name))
                    tableau.users.populate_workbooks(old_user)
                    if len(old_user.workbooks) > 0:
                        old_user.site_role = 'Unlicensed'
                        tableau.users.update(old_user)
                    else:
                        tableau.users.remove(old_user.id)

            logger.info("New users: {0}".format(new_users_set))
            if do_something:
                for new_user in new_users_set:
                    if new_user in [user.name for user in tableau_unlicensed_users]:
                        tableau_user = [user for user in tableau_unlicensed_users if user.name == new_user].pop()
                        logger.info('Change site role to Interactor for {0}'.format(new_user))
                        tableau_user.site_role = 'Interactor'
                        tableau.users.update(tableau_user)

                    else:
                        logger.debug("Creating user: {0}".format(new_user))
                        ad_user_data = ad.get_user_by_samaccountname(new_user)
                        password = ''.join(random.choice(string.ascii_letters + string.digits) for x in range(32))
                        new_user = TSC.UserItem(name=ad_user_data[0].sAMAccountName.value,
                                            site_role='Interactor')
                        new_user = tableau.users.add(new_user)
                        new_user.email = ad_user_data[0].mail.value
                        new_user.fullname = ad_user_data[0].name.value
                        new_user.password = password
                        new_user = tableau.users.update(new_user)


            tableau_all_site_users = [user for user in TSC.Pager(tableau.users)]
            for site_user in [user for user in tableau_all_site_users if user.site_role != 'Unlicensed']:
                if not any(site_user.name in tableau_service_account for tableau_service_account in
                           tableau_service_accounts):
                    tableau_user = tableau.users.get_by_id(site_user.id)
                    ad_user = ad.get_user_by_samaccountname(site_user.name)
                    if ad_user != []:
                        if tableau_user.fullname != ad_user[0].name.value and do_something:
                            logger.info("Changing userdata {0}".format(tableau_user.name))
                            tableau_user.fullname = ad_user[0].name.value
                            tableau_user.email = ad_user[0].mail.value
                            tableau.users.update(tableau_user)

            logger.info('Revision groups on site')
            ad_site_groups = [group.name.value for group in ad.get_site_groups(current_site)]
            tableau_site_groups = [group for group in TSC.Pager(tableau.groups)]
            new_groups = set(ad_site_groups) - set(
                [tablesu_site_group.name for tablesu_site_group in tableau_site_groups])
            old_groups = set([tableau_site_group.name for tableau_site_group in tableau_site_groups]) - set(
                ad_site_groups)

            #This ugly code, but I was forced to write this.
            if current_site.name == 'ERS':
                old_groups = [t for t in old_groups if not(t.startswith('F_') or t.startswith('A_'))]
            # End ugly code

            old_groups.remove('All Users')
            logger.info("New groups {0}".format(new_groups))
            if do_something:
                for new_group in new_groups:
                    newgroup = TSC.GroupItem(new_group)
                    tableau.groups.create(newgroup)

            logger.info("Old groups {0}".format(old_groups))
            if do_something:
                for old_group in old_groups:
                    group_id = [group.id for group in tableau_site_groups if group.name == old_group]
                    tableau.groups.delete(group_id.pop())

            logger.info('Revision group members on site')
            all_tableau_groups = [group for group in TSC.Pager(tableau.groups)]
            opts = TSC.RequestOptions(pagesize=1000)

            # This ugly code, but I was forced to write this.
            if current_site.name == 'ERS':
                all_tableau_groups = [t for t in all_tableau_groups if
                                      not (t.name.startswith('F_') or t.name.startswith('A_'))]
            # End ugly code

            for group in all_tableau_groups:
                if group.name != 'All Users':
                    tableau.groups.populate_users(group, opts)
                    tableau_members_set = set([user.name for user in group.users])
                    ad_members = ad.get_members_by_groupname(group.name)
                    ad_members_set = set([user.sAMAccountName.value for user in ad_members])
                    new_members_set = ad_members_set - tableau_members_set
                    old_members_set = tableau_members_set - ad_members_set

                    if new_members_set != set():
                        logger.info("{0}, new members:{1} ".format(group.name, new_members_set))
                    if old_members_set != set():
                        logger.info("{0}, old members{1}".format(group.name, old_members_set))

                    if do_something:
                        for new_member in new_members_set:
                            logger.debug("Adding user:{0}".format(new_member))
                            user_id = [user.id for user in tableau_all_site_users if user.name == new_member].pop()
                            tableau.groups.add_user(user_id=user_id, group_item=group)

                    if do_something:
                        for old_member in old_members_set:
                            logger.debug("Removing user:{0}".format(old_member))
                            user_id = [user.id for user in tableau_all_site_users if user.name == old_member][0]
                            tableau.groups.remove_user(user_id=user_id, group_item=group)

    if 'Zabbix' in config_sections:
        zabbix_config_path = config.get('Zabbix', 'zabbix_agentd_conf')
        zabbix_config = open(zabbix_config_path).read()
        zabbix_server = re.search( r'ServerActive=(.+)', zabbix_config).group(1)
        zabbix_item = config.get('Zabbix', 'item')
        zabbix_hostname = re.search( r'Hostname=(.+)', zabbix_config).group(1)
        server = ZabbixSender(zabbix_server, 10051)
        packet = ZabbixPacket()
        packet.add(zabbix_hostname, zabbix_item, '1')
        server.send(packet)


if __name__ == '__main__':
    sys.exit(main())

