#!/usr/bin/env python3
# Receives user, password, local temp directory, remote path in home, path to the binary,
# template for dir names, computational cluster name;
# then open connection to the remote server and upload data.
# Kuklin E.Y. 2022

import sys
import os
import paramiko
import time
from loguru import logger


class SetSettings:
    def __init__(self):
        self.IMM_URL = 'some/url/1'
        self.URFU_URL = 'some/url/2'
        self.PORT_IMM = 22
        self.PORT_URFU = 22
        self.USER = sys.argv[1]
        self.PASSWD = sys.argv[2]
        self.LOCAL_PATH = sys.argv[3]
        self.REMOTE_PATH = sys.argv[4]
        self.BINARY_PATH = sys.argv[5]
        self.ALTERNATENAME = sys.argv[6]
        self.CLUSTER = sys.argv[7]

        if self.CLUSTER == 'urfu':
            self.HOST = self.URFU_URL
            self.PORT = self.PORT_URFU
        else:
            self.HOST = self.IMM_URL
            self.PORT = self.PORT_IMM

        self.HEAD, self.TAIL = os.path.split(self.LOCAL_PATH)
        self.CLIENT = None

    def open_connection(self):
        self.CLIENT = paramiko.SSHClient()
        self.CLIENT.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            self.CLIENT.connect(hostname=self.HOST, username=self.USER,
                                password=self.PASSWD, port=self.PORT)
            logger.success('Connection opened: ' + self.USER + ', ' + self.CLUSTER)
        except paramiko.SSHException:
            logger.error('Connection failed: ' + self.USER + ', ' + self.CLUSTER)
            sys.exit()

    def close_connection(self):
        self.CLIENT.close()
        sys.exit()


def put_all(self, local_path, remote_path):
    """Recursively uploads a directory"""
    os.chdir(os.path.split(local_path)[0])
    parent = os.path.split(local_path)[1]
    for walker in os.walk(parent):
        # .replace('\\', '/') makes unix-type path in case of Win user
        self.mkdir(os.path.join(remote_path, walker[0]).replace('\\', '/'))
        for file in walker[2]:
            self.put(os.path.join(walker[0], file), os.path.join(remote_path, walker[0], file).replace('\\', '/'))


def upload_directory(client):
    try:
        client.CLIENT.exec_command('mkdir -p ' + client.REMOTE_PATH)
        time.sleep(1)
        sftp = client.CLIENT.open_sftp()
        put_all(sftp, client.LOCAL_PATH, client.REMOTE_PATH)
        sftp.close()
        logger.success('Uploading data done.')
    except paramiko.SSHException:
        logger.error('Cannot upload the target directory. Some error occurred...')
        client.close_connection()


def create_link(client):
    try:
        client.CLIENT.exec_command('ln -s ' + client.BINARY_PATH + ' ' +
                                   client.REMOTE_PATH + '/' + client.TAIL + '/binary')
        logger.success('Link created.')
        time.sleep(1)
    except paramiko.SSHException:
        logger.error('Link creation failed.')
        client.close_connection()


def copy_source(client):
    try:
        client.CLIENT.exec_command('cd ' + client.REMOTE_PATH + '/' + client.TAIL +
                                   '; binpath=$(dirname `readlink binary`); (echo $binpath; mkdir source; ' +
                                   'cp -v $binpath/{*.c,*.h,*akefile} source; ' +
                                   'cp -v $(readlink binary) source) &> logfile;')
        logger.success('Source code copied.')
    except paramiko.SSHException:
        logger.error('Copying of the source code failed.')
        client.close_connection()


def config_multiplier(client):
    try:
        channel = client.CLIENT.get_transport().open_session()
        channel.exec_command('cd ' + client.REMOTE_PATH + '/' + client.TAIL +
                             '; (chmod 755 config_multiplier.php; ./config_multiplier.php ' +
                             client.ALTERNATENAME + ') &>> logfile;')
        # Wait for it to finish
        while not channel.exit_status_ready():
            time.sleep(1)
        logger.success('config_multiplier done.')
    except paramiko.SSHException:
        logger.error('config_multiplier failed.')
        client.close_connection()


def launch_runme(client):
    try:
        # Execute some remote commands
        channel = client.CLIENT.get_transport().open_session()
        channel.exec_command('cd ' + client.REMOTE_PATH + '/' + client.TAIL +
                             '; (. ~/.bashrc; . ~/.bash_profile; chmod 755 runme.sh; ./runme.sh) &>> logfile;')
        # Wait for it to finish
        while not channel.exit_status_ready():
            time.sleep(1)
        logger.success('runme done.')
    except paramiko.SSHException:
        logger.error('runme failed.')
        client.close_connection()


def get_jobs_ids(client):
    try:
        stdin, stdout, stderr = client.CLIENT.exec_command(
            'cd ' + client.REMOTE_PATH + '/' + client.TAIL +
            '; cat logfile | grep "Submitted batch job" | awk \'{ print $NF }\' ORS=","', get_pty=1)
        jobs_ids = stdout.read()
        # Get rid of the b-prefix and '
        print(jobs_ids.decode('utf-8'))
    except paramiko.SSHException:
        logger.error("Error in job ID retrieving")
        client.close_connection()


if __name__ == '__main__':
    Client = SetSettings()
    Client.open_connection()
    upload_directory(Client)
    create_link(Client)
    copy_source(Client)
    config_multiplier(Client)
    launch_runme(Client)
    get_jobs_ids(Client)
    Client.close_connection()
