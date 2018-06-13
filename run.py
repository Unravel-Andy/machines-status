#v1.1.7
from confluence import Confluence
from base64 import b64encode as b64e
import subprocess
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-user', '--username', help='Confluence Login', required=True)
parser.add_argument('-pass', '--password', help='Confluence Password', required=True)
argv = parser.parse_args()


def auto_update():
    print('Updating Script\n')
    path = '--git-dir=/usr/bin/machines-status/.git'
    result = subprocess.check_output('git ' + path +' checkout -- .',shell=True)
    print(result)
    result = subprocess.check_output('git ' + path +' pull',shell=True)
    print(result)


def main():
    auto_update()
    unravel_base_url = 'http://localhost:3000'
    atlassian_base_url = 'https://unraveldata.atlassian.net/wiki/rest/api/content/284262500'
    atlassian_base_url2 = 'https://unraveldata.atlassian.net/wiki/rest/api/content/502628605'
    credentials = b64e('%s:%s' % (argv.username, argv.password))

    confluence = Confluence(unravel_base_url, atlassian_base_url, credentials)
    confluence2 = Confluence(unravel_base_url, atlassian_base_url2, credentials)
    
    unravel_ver = confluence.unravel_ver()
    unravel_ver = confluence2.unravel_ver()
    print('Current unravel_ver is: %s\n' % unravel_ver)

    content = confluence.get_content_ver()
    print('Editing Content Version: %s\n' % content)
    content2 = confluence2.get_content_ver()
    print('Editing Content2 Version: %s\n' % content2)

    body = confluence.get_content_body()
    print('Editing Content Title: %s' % body['title'])
    print('Editing Content Status: %s' % body['stat'])
    print('Editing Content type: %s\n' % body['type'])
    body2 = confluence2.get_content_body()
    print('Editing Content Title: %s' % body2['title'])
    print('Editing Content Status: %s' % body2['stat'])
    print('Editing Content type: %s\n' % body2['type'])

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

    # Check whether test-cluster page need update or not
    if confluence2.set_content() == True:
        print('should process and put content in %s' % body2['title'])
        result = confluence2.put_content()
        if result:
            print('Put new info to Confluence Success')
        else:
            print('Put new info to Confluence Fail')

    else:
        print('%s no change should not process\n' % body2['title'])


if __name__ == '__main__':
    main()
