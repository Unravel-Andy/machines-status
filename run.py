#v1.1.8
from confluence import Confluence
from base64 import b64encode as b64e
import subprocess
import argparse
import os
import host_to_db

parser = argparse.ArgumentParser()
parser.add_argument('-user', '--username', help='Confluence Login', required=True)
parser.add_argument('-pass', '--password', help='Confluence Password', required=True)
parser.add_argument('-alias', '--alias', help='Alias name for the machine', default=None)
argv = parser.parse_args()


def auto_update():
    print('Updating Script\n')
    os.chdir('/usr/bin/machines-status/')
    path = '--git-dir=/usr/bin/machines-status/.git'
    result = subprocess.check_output('git ' + path +' checkout -- .',shell=True)
    print(result)
    result = subprocess.check_output('git ' + path +' pull',shell=True)
    print(result)


def update_confluence(atlassian_base_url):
    unravel_base_url = 'http://localhost:3000'
    credentials = b64e('%s:%s' % (argv.username, argv.password))

    confluence = Confluence(unravel_base_url, atlassian_base_url, credentials, alias_name=argv.alias)

    unravel_ver = confluence.unravel_ver()
    print('Current unravel_ver is: %s\n' % unravel_ver)

    content = confluence.get_content_ver()
    print('Editing Content Version: %s\n' % content)

    body = confluence.get_content_body()
    print('Editing Content Title: %s' % body['title'])
    print('Editing Content Status: %s' % body['stat'])
    print('Editing Content type: %s\n' % body['type'])

    # Check whether live machine status page need update or not
    if confluence.set_content() == True:
        print('should process and put content in %s' % body['title'])
        result = confluence.put_content()
        if result:
            print('Put new info to Confluence Success')
        else:
            print('Put new info to Confluence Fail')
    else:
        print('%s content no change should not process\n' % body['title'])


def main():
    auto_update()
    update_confluence(atlassian_base_url='https://unraveldata.atlassian.net/wiki/rest/api/content/284262500')
    update_confluence(atlassian_base_url='https://unraveldata.atlassian.net/wiki/rest/api/content/502628605')
    print("Sending configs to Database")
    host_to_db.send_to_db(alias_name=argv.alias)


if __name__ == '__main__':
    main()
