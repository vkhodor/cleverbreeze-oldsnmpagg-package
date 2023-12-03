# -*- coding: utf-8 -*-
# Project: snmpagg
# File: config
# Author: Victor V. Khodorchenko
# Mail: hvv@nsu.ru
# Year: 2017

import hashlib
import os
import socket

try:
    from config import GROUPS_FILE
except ModuleNotFoundError:
    GROUPS_FILE = 'groups'

CONNECT_TEST_DELAY = 1

def lower_first_char(string):
    try:
        return string[:1].lower() + string[1:]
    except:
        return string

def verify_login(login, password, st_users, logger=None):
    """
    Func verify login/password in [{'user': 'md5hash'}, {'user1': 'md5hash1'}...]
    :return: True if OK else False
    """
    md5_password = hashlib.md5(password.encode('utf-8')).hexdigest()

    debug(logger, 'login={0}, password={1}, md5hash={2}'.format(login, password, md5_password))
    if {login: md5_password} in st_users:
        return True
    return False


def save_groups_file(groups, groups_file=GROUPS_FILE, logger=None):
    try:
        f = open('{0}.tmp'.format(groups_file), 'w')
        lines = []
        for group_name in groups.keys():
            line = '{0}: {1}\n'
            users_line = ', '.join(groups[group_name])
            lines.append(line.format(group_name, users_line))
        f.writelines(lines)

        os.remove(groups_file)
        os.rename('{0}.tmp'.format(groups_file), groups_file)

    except Exception as e:
        err(logger, 'Error in save_groups_file: {0}'.format(e))
        return False


def get_groups(groups_file, logger=None):
    """
    Функция берет группы из файла вида
    group_name: user1, user2,...
    group_name1: user3, user1,...

    Возвращает словарь вида:
        {
            "group_name": ["user1", "user2"]
            "group_name1": ["user2", "user1"]
        }
    """
    groups = dict()

    debug(logger, 'GROUPS_FILE = {0}'.format(groups_file))
    try:
        f = open(groups_file)
        for i, line in enumerate(f.readlines()):
            line = line.strip().replace(' ', '')
            debug(logger, line)
            if len(line) <= 2:
                continue

            splited_line = line.split(':')
            if len(splited_line) != 2:
                warn(logger, 'Line {0} is not correct!'.format(i+1))
                continue

            groups[splited_line[0]] = splited_line[1].split(',')
        f.close()

    except Exception as e:
        err(logger, 'get_group exception: {0} (line: {1})'.format(e, i+1))

    return groups


def add_in_group(username, group_name, groups):
    if group_name not in groups.keys():
        groups[group_name] = [username]
    else:
        groups[group_name].append(username)

    return groups


def remove_from_group(username, group_name, groups):
    if username in groups[group_name]:
        groups[group_name].remove(username)

    return groups


def remove_from_all_groups(username, groups):
    for group_name in groups.keys():
        if username in groups[group_name]:
            groups[group_name].remove(username)

    return groups


def get_users(users_file, logger=None):
    """
    Функция берет пользователей из текстового файла вида:
    user_name:md5pass
    user2:md5pass2

    :return: [{'user': 'md5hash'}, {'user1': 'md5hash1'}...]
    """
    debug(logger, 'USERS_FILE = {0}'.format(users_file))
    f = open(users_file)

    users = []
    uids = []
    for line in f.readlines():
        line = line.strip()
        debug(logger, line)
        if len(line) > 2:
            splited_line = line.split(':')
            if len(splited_line) == 2:
                users.append({splited_line[0]: splited_line[1]})
                uids.append(splited_line[0])
            else:
                warn(logger, 'Not correct line. Skip.')
                continue
        else:
            warn(logger, 'Empty line. Skip.')
            continue

    return uids,users


def remove_user(users_file, username=None, logger=None):
    """Функция удаляет пользователя из файла"""

    if username == None:
        return False

    uids,users = get_users(users_file, logger)

    if username not in uids:
        return False

    f = open(users_file + '.tmp', 'w')

    for user in  users:
        uid = list(user.keys())[0]
        if username != uid:
            f.write('{0}:{1}\n'.format(uid, user[uid]))

    f.close()

    os.remove(users_file)
    os.rename(users_file + '.tmp', users_file)

    return True


def user_add_or_modify(users_file, username=None, password='', logger=None):
    """Функция добавляет пользователя в файл если он там есть то меняет хэш"""

    if username == None:
        return False

    uids,users=get_users(users_file=users_file, logger=logger)

    md5_password = hashlib.md5(password.encode('utf-8')).hexdigest()
    debug(logger, '{0} -> {1}'.format(password, md5_password))

    if username not in uids:
        users.append({username: md5_password})
        debug(logger, 'Add user! {0}:{1}'.format(username, md5_password))
    else:
        for user in users:
            if username in user.keys():
                user[username] = md5_password
                debug(logger, 'Modify user {0}:{1}'.format(username, md5_password))


    f = open(users_file + '.tmp', 'w')

    debug(logger, 'Writing tmp-file: {0}...'.format(users_file + '.tmp'))
    for user in users:
        uid = list(user.keys())[0]
        f.write('{0}:{1}\n'.format(uid, user[uid]))

    f.close()
    os.remove(users_file)
    debug(logger, 'Rename tmp-file to {0}...'.format(users_file))
    os.rename(users_file + '.tmp', users_file)
    return True


def is_valid_ip(str_ip):
    """
    Функция проверяет является ли строка валидным IP-адресом
    :param str_ip:
    :return: True если да и False если нет
    """

    octets = str_ip.split('.')

    if len(octets) != 4:
        return False

    if int(octets[3]) == 0 or int(octets[3]) == 255:
        return False

    for octet in octets:
        if int(octet) <0 or int(octet) > 254:
            return False

    return True


def debug(logger, msg):
    if logger is not None:
        logger.debug(msg)
    else:
        return


def info(logger, msg):
    if logger is not None:
        logger.info(msg)
    else:
        return


def warn(logger, msg):
    if logger is not None:
        logger.warn(msg)
    else:
        return


def err(logger, msg):
    if logger is not None:
        logger.warn(msg)
    else:
        return
