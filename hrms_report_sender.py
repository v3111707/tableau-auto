#!/usr/bin/env python3.7

import requests
import logging
import typer
import re
import json
import sys
import smtplib
import ssl
import os
import datetime
from typing import Optional
from logging.handlers import RotatingFileHandler
from pyzabbix import ZabbixMetric, ZabbixSender
import tableauserverclient as TSC
from urllib.parse import urljoin
from dotenv import dotenv_values
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from jinja2 import Environment, FileSystemLoader

SCRIPT_NAME = os.path.basename(__file__)


def init_logger(debug: bool = False, log_names: list = None, path: str = None):
    if log_names is None:
        log_names = ['main']
    for log_name in log_names:
        logger = logging.getLogger(log_name)
        logger.setLevel(logging.DEBUG)
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s: %(message)s'))
        if debug:
            sh.setLevel(logging.DEBUG)

        else:
            sh.setLevel(logging.INFO)
        logger.addHandler(sh)
        if path:

            fh = RotatingFileHandler(path,
                                     maxBytes=4194304,
                                     backupCount=3)
            fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            # fh.setLevel(logging.DEBUG)
            logger.addHandler(fh)
    logger.debug('Set level DEBUG')


def convert_date(date: str):
    timestamp = int(re.findall(r'\((\d+)\D', date)[0])
    return datetime.datetime.fromtimestamp(timestamp / 1000)


class ZabSender(object):
    def __init__(self, item_key: str, config_file:str = '/etc/zabbix/zabbix_agentd.conf'):
        self.logger = logging.getLogger('main.Zabbix_sender')
        self.item_key = item_key
        zabbix_config = open(config_file).read()
        self.server = re.search(r'ServerActive=(.+)', zabbix_config).group(1)
        # self.logger.debug(f"self.server: {self.server}")
        self.hostname = re.search(r'Hostname=(.+)', zabbix_config).group(1)
        # self.logger.debug(f"self.hostname: {self.hostname}")

    def send(self, value):
        packet = [ZabbixMetric(self.hostname, self.item_key, value)]
        self.logger.info(f"Send {packet} to {self.server}")
        return ZabbixSender(zabbix_server=self.server).send(packet)


class MailStatus:
    _first_mail = 'first_mail'
    _second_mail = 'second_mail'
    _third_mail = 'third_mail'
    _logger_name = 'main.MailStatus'

    def __init__(self, path: str):
        self.logger = logging.getLogger(self._logger_name)
        self._path = path
        self._data = {}
        if os.path.isfile(path):
            self.logger.info(f'Loading {path}')
            with open(self._path, "r") as f:
                self._data = json.load(f)

    def _save_data(self):
        with open(self._path, "w") as f:
            json.dump(self._data, f)

    def get_first_mail_state(self, username: str):
        if self._data.get(username) and self._data.get(username).get(self._first_mail):
            return True
        return False

    def get_second_mail_state(self, username: str):
        if self._data.get(username) and self._data.get(username).get(self._second_mail):
            return True
        return False

    def get_third_mail_state(self, username: str):
        if self._data.get(username) and self._data.get(username).get(self._third_mail):
            return True
        return False

    def set_first_mail_state(self, username: str):
        self.logger.info(f'Set first mail to True for {username}')
        self._data[username] = {**self._data.get(username, {}), **{self._first_mail: True}}
        self._save_data()

    def set_second_mail_state(self, username: str):
        self.logger.info(f'Set second mail to True for {username}')
        self._data[username] = {**self._data.get(username, {}), **{self._second_mail: True}}
        self._save_data()

    def set_third_mail_state(self, username: str):
        self.logger.info(f'Set third mail to True for {username}')
        self._data[username] = {**self._data.get(username, {}), **{self._third_mail: True}}
        self._save_data()

    def clean(self, username: str):
        if username in self._data:
            self.logger.info(f'Clean mail status for {username}')
            self._data.pop(username)
            self._save_data()


class EmailSender:
    _logger_name = 'main.EmailSender'

    def __init__(self, host: str = '', username: str = None, password: str = None, port: int = 25, use_ssl: bool = True,
                 sender: str = None):
        self.logger = logging.getLogger(self._logger_name)
        if not sender:
            sender = username
        self.server = host
        self.username = username
        self.password = password
        self.port = port
        self.use_ssl = use_ssl
        self.sender = sender
        self.context = ssl.create_default_context()

    def send_mail(self, to: list, subject: str, msg_plain: str = None, msg_html: str = None):
        message = MIMEMultipart('mixed')
        message['to'] = ", ".join(to)
        message['from'] = self.sender
        message['subject'] = subject

        message_alternative = MIMEMultipart('alternative')
        message_related = MIMEMultipart('related')
        if msg_html:
            message_related.attach(MIMEText(msg_html, 'html'))
        if msg_plain:
            message_alternative.attach(MIMEText(msg_plain, 'plain'))
        message_alternative.attach(message_related)
        message.attach(message_alternative)

        self.logger.debug(f'message: {message}')
        with smtplib.SMTP(host=self.server, port=self.port) as server:
            server.ehlo()
            server.starttls(context=self.context)
            # ehlo_resp = server.ehlo()
            # self.logger.debug(f'{ehlo_resp=}')
            if self.username and self.password:
                server.login(user=self.username, password=self.password)
            # self.logger.info(f'Sending mail form "{self.sender}" to; "{to}" \n {message.as_string()}')
            self.logger.info(f'Sending mail form "{self.sender}" to; "{to}" ')
            sendmail_resp = server.sendmail(from_addr=self.sender, to_addrs=to, msg=message.as_string())
            self.logger.debug(f'sendmail_resp: {sendmail_resp}')
        return sendmail_resp

    def load_template(self, template: str):
        dirname = os.path.dirname
        templates_dir = dirname(os.path.realpath(__file__))
        env = Environment(loader=FileSystemLoader(templates_dir))
        return env.get_template(template)

    def _render_data(self, data: dict, template_name: str):
        template = self.load_template(template_name)
        rendered = template.render(**data)
        return rendered

    def send_templated_mail(self, to: list, subject: str, template_name: str, data: dict):
        body = self._render_data(data=data, template_name=template_name)
        return self.send_mail(to=to, subject=subject,  msg_html=body)

    def __enter__(self):
        return self

    def __exit__(self, exc, val, tb):
        pass


class SuccessFactorsClient:
    logger_name = 'main.SFC'

    def __init__(self, url: str):
        self.log = logging.getLogger(self.logger_name)
        self.base_url = url
        self.base_headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        self.token = {}
        self.log.debug('End init')

    def auth(self, company_id: str, client_id: str, assertion: str,
             grant_type: str = 'urn:ietf:params:oauth:grant-type:saml2-bearer'):
        resource_path = 'oauth/token'
        url = urljoin(self.base_url, resource_path)
        headers = self.base_headers

        params = {
            'company_id': company_id,
            'client_id': client_id,
            'grant_type': grant_type,
            'assertion': assertion
        }

        resp = requests.post(url=url, params=params, headers=headers)
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as exp:
            if resp.status_code == 401:
                logging.error(resp.json())
            raise exp
        self.token = resp.json()

    def get_leaving_users(self, up_to: datetime):
        resource_path = 'odata/v2/EmpJob'
        url = urljoin(self.base_url, resource_path)
        token_type = self.token['token_type']
        access_token = self.token['access_token']
        # from_date = str(datetime.datetime.now().date())
        from_date = str(datetime.datetime.now() + datetime.timedelta(days=-7))

        params = {'format': 'json',
                  '$select': 'userId,managerId,customDate4',
                  'toDate': str(up_to.date()),
                  '$filter': f"customDate4 ge '{from_date}' "
                             f"and userNav/status in 't','f','T','F','e','d'"}

        headers = {'Authorization': f'{token_type} {access_token}',
                   'accept': 'application/json'}

        resp = requests.get(url=url, headers=headers, params=params)
        resp.raise_for_status()
        body = resp.json()
        users = body['d']['results']
        return users

    def get_user_by_id(self, user_id: str):
        resource_path = f"odata/v2/User('{user_id}')"
        url = urljoin(self.base_url, resource_path)

        token_type = self.token['token_type']
        access_token = self.token['access_token']
        headers = {'Authorization': f'{token_type} {access_token}',
                   'accept': 'application/json'}

        params = {'format': 'json',
                  '$select': 'userId,displayName,email,username'}

        resp = requests.get(url=url, headers=headers, params=params)
        body = resp.json()['d']
        body.pop('__metadata')
        return body


app = typer.Typer(add_completion=False)
@app.command(context_settings=dict(help_option_names=["-h", "--help"]))
def cli(debug: Optional[bool] = typer.Option(False, '-d', '--debug', show_default=True),
        mail_to: Optional[str] = typer.Option(None, '-m', '--mail_to', show_default=False),
        print_data: Optional[bool] = typer.Option(False, '-p', show_default=True,
                                                  help='Print data from HRMS and exit'),
        zab_test: Optional[bool] = typer.Option(False, '--zt', show_default=True,
                                                help='Send to zabbox "1"'),
        load_file: Optional[str] = typer.Option(None, '-l', show_default=True)):

    init_logger(debug=debug, log_names=['main'], path=SCRIPT_NAME + '.log')
    log = logging.getLogger('main')

    exit_code = 0

    hrms_creds = dotenv_values('.env.hrms.creds')
    tableau_creds = dotenv_values('.env.tableau.creds')
    mail_creds = dotenv_values('.env.email.creds')
    script_conf = dotenv_values('.env.hrms_report_sender')

    mail_states = MailStatus('hrms_report_sender_email_states.json')

    tableau_url = tableau_creds['url']

    if zab_test:
        zs = ZabSender(item_key=SCRIPT_NAME)
        zs.send(1)
        sys.exit(0)

    if load_file:
        log.info(f'Opening "{os.path.abspath(load_file)}"')
        with open(os.path.abspath(load_file), 'r') as f:
            report_data = json.load(f)
        for i in report_data:
            i['termination_date'] = datetime.datetime.strptime(i['termination_date'], '%Y-%m-%d %H:%M:%S')

    else:
        sfc = SuccessFactorsClient(hrms_creds.pop('url'))
        sfc.auth(**hrms_creds)
        users = sfc.get_leaving_users(up_to=datetime.datetime.now() + datetime.timedelta(days=30))
        report_data = []
        log.info('Leaving users in hrms:')
        for u in users:
            user_id = u['userId']
            manager_id = u['managerId']
            termination_date = convert_date(u['customDate4'])
            user_data = sfc.get_user_by_id(user_id)
            manager_data = sfc.get_user_by_id(manager_id)
            username = user_data['username']

            report_data.append({**user_data, **{'manager': manager_data,
                                                'termination_date': termination_date,
                                                'tableau_url': tableau_url,
                                                'tableau_resources': {}}})
            log.info(
                f'User:{username},  termination date:{termination_date.date()}, manager\'s email: {manager_data["email"]}')
    if print_data:
        print(json.dumps(report_data, indent=2, default=str))
        return

    tableau_auth = TSC.TableauAuth(username=tableau_creds['username'],
                                   password=tableau_creds['password'])
    server = TSC.Server(server_address=tableau_url,
                        use_server_version=True)

    with server.auth.sign_in(tableau_auth):
        for site in TSC.Pager(server.sites):
            server.auth.switch_site(site)
            all_projects = list(TSC.Pager(server.projects))
            all_projects = {i.id: i for i in all_projects}
            project_id_path = {}
            for _, project in all_projects.items():
                path = []
                parent_id = project.parent_id
                while True:
                    if parent_id:
                        path.append(all_projects[parent_id].name)
                        parent_id = all_projects[parent_id].parent_id
                    else:
                        break
                path.append('Home')
                path.reverse()
                project_id_path[project.id] = ' / '.join(path)

            log.debug(f'Processing site: {site.name} ({server.site_id})')
            for user_data in report_data:
                if site.content_url:
                    user_content_url = f'{server.server_address}#/site/{site.content_url}/user/local/{user_data["username"]}/content'
                else:
                    user_content_url = f'{server.server_address}#/user/local/{user_data["username"]}/content'
                # user_data['user_content_url'] = user_content_url

                req_option = TSC.RequestOptions()
                req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.OwnerEmail,
                                                 TSC.RequestOptions.Operator.Equals,
                                                 user_data['email']))

                workbooks = list(TSC.Pager(server.workbooks, req_option))
                projects = list(TSC.Pager(server.projects, req_option))
                if any([workbooks, projects]):
                    user_data['tableau_resources'][site.name] = {}
                    user_data['tableau_resources'][site.name]['user_content_url'] = user_content_url
                    user_data['tableau_resources'][site.name]['workbooks'] = [{'name': w.name,
                                                                               'project_name': w.project_name}
                                                                              for w in workbooks]
                    user_data['tableau_resources'][site.name]['projects'] = [{'name': p.name,
                                                                              'path': project_id_path[p.id]}
                                                                             for p in projects]

    with EmailSender(**mail_creds) as mail_sender:
        for user_data in report_data:
            days_left = (user_data['termination_date'] - datetime.datetime.now()).days
            username = user_data['username']
            log.info(f'Processing {user_data["displayName"]}, days_left: {days_left}')
            if mail_to:
                recipients = mail_to.split(',')
            else:
                # recipients = list(set(script_conf['mail_to'].split() + [user_data['manager']['email'],  user_data['email']]))
                recipients = mail_to.split(',')
            subject = f"Moving {tableau_url.replace('https://', '')} reports of the user leaving the company"

            if days_left < -5:
                log.info('days_left less then 5')
                mail_states.clean(username)
            elif not user_data.get('tableau_resources'):
                log.info('tableau_resources is None. Ignore')
            elif days_left < 0 and not mail_states.get_third_mail_state(username):
                log.info(f'Send third mail to {recipients}')
                resp = mail_sender.send_templated_mail(to=recipients,
                                                       subject=subject,
                                                       template_name=script_conf['mail_template'],
                                                       data=user_data)
                log.info(f'send_templated_mail resp: {resp}')
                mail_states.set_third_mail_state(username)
            elif days_left < 7 and not mail_states.get_second_mail_state(username):
                log.info(f'Send second mail to {recipients}')
                resp = mail_sender.send_templated_mail(to=recipients,
                                                       subject=subject,
                                                       template_name=script_conf['mail_template'],
                                                       data=user_data)
                log.info(f'send_templated_mail resp: {resp}')
                mail_states.set_second_mail_state(username)
            elif days_left > 7 and not mail_states.get_first_mail_state(username):
                log.info(f'Send first mail to {recipients}')
                resp = mail_sender.send_templated_mail(to=recipients,
                                                       subject=subject,
                                                       template_name=script_conf['mail_template'],
                                                       data=user_data)
                log.debug(f'send_templated_mail resp: {resp}')
                mail_states.set_first_mail_state(username)

    try:
        zs = ZabSender(item_key=SCRIPT_NAME)
        log.info(f'Send to zabbix "{exit_code}"')
        zs.send(exit_code)
    except Exception as e:
        log.warning(f'Exception while send to Zabbix:{e}')


if __name__ == "__main__":
    app()
