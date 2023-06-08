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
import tableauserverclient as TSC
from urllib.parse import urljoin
from dotenv import dotenv_values
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from jinja2 import Environment, FileSystemLoader


def init_logger(debug: bool = False, log_names: list = None):
    if log_names is None:
        log_names = ['main']
    for log_name in log_names:
        logger = logging.getLogger(log_name)
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s: %(message)s'))
        logger.addHandler(sh)
        if debug:
            logger.setLevel(logging.DEBUG)
            logger.debug('Set level DEBUG')
        else:
            logger.setLevel(logging.INFO)


def convert_date(date: str):
    timestamp = int(re.findall(r'\((\d+)\D', date)[0])
    return datetime.datetime.fromtimestamp(timestamp / 1000)


class MailStatus:
    _first_mail = 'first_mail'
    _second_mail = 'second_mail'

    def __init__(self, path: str):
        self._path = path
        self._data = {}
        if os.path.isfile(path):
            with open(self._path, "r") as f:
                self._data = json.load(f)

    def _save_data(self):
        with open(self._path, "w") as f:
            json.dump(self._data, f)

    def get_first_mail_state(self, username):
        if self._data.get(username) and self._data.get(username).get(self._first_mail):
            return True
        return False

    def get_second_mail_state(self, username):
        if self._data.get(username) and self._data.get(username).get(self._second_mail):
            return True
        return False

    def set_first_mail_state(self, username):
        self._data[username] = self._data.get(username, {}) | {self._first_mail: True}
        self._save_data()

    def set_second_mail_state(self, username):
        self._data[username] = self._data.get(username, {}) |{self._second_mail: True}
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
            self.logger.info(f'Sending mail form "{self.sender}" to; "{to}" \n {message.as_string()}')
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

        params = {'format': 'json',
                  '$select': 'userId,managerId,customDate4',
                  'toDate': str(up_to.date()),
                  '$filter': f"customDate4 ge '{str(datetime.datetime.now().date())}' "
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
def cli(debug: Optional[bool] = typer.Option(True, '-d', '--debug', show_default=True),
        mail_to: Optional[str] = typer.Option(False, '-m', '--mail_to', show_default=False),
        print_data: Optional[bool] = typer.Option(False, '-p', show_default=True, help='Print data from HRMS and exit'),
        load_file: Optional[str] = typer.Option(None, '-l', show_default=True)):

    init_logger(debug=debug, log_names=['main'])
    log = logging.getLogger('main')

    mail_states = MailStatus('email_states.json')

    hrms_conf = dotenv_values('.env.hrms')
    tab_conf = dotenv_values('.env.tableau')
    mail_conf = dotenv_values('.env.email')
    script_conf = dotenv_values('.env.hrms_report_sender')

    tableau_url = tab_conf['url']

    if load_file:
        log.info(f'Opening "{os.path.abspath(laod_file)}"')
        with open(os.path.abspath(laod_file), 'r') as f:
            report_data = json.load(f)
    else:
        sfc = SuccessFactorsClient(hrms_conf.pop('url'))
        sfc.auth(**hrms_conf)
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

            report_data.append(user_data | {'manager': manager_data,
                                            'termination_date': termination_date,
                                            'tableau_url': tableau_url,
                                            'tableau_resources': {}})
            log.info(
                f'User:{username},  termination date:{termination_date.date()}, manager\'s email: {manager_data["email"]}')
    if print_data:
        print(report_data)
        return


if __name__ == "__main__":
    app()
