#v1.1.3
from confluence import Confluence
from base64 import b64encode as b64e
import subprocess

def main():
    auto_update()
    unravel_base_url = 'http://localhost:3000'
    atlassian_base_url = 'https://unraveldata.atlassian.net/wiki/rest/api/content/284262500'
    credentials = b64e('team@unraveldata.com:Unraveldata123!')

    confluence = Confluence(unravel_base_url, atlassian_base_url, credentials)

    unravel_ver = confluence.unravel_ver()
    print('Current unravel_ver is: %s\n' % unravel_ver)

    content = confluence.get_content_ver()
    print('Editing Content Version: %s\n' % content)

    body = confluence.get_content_body()
    print('Editing Content Title: %s' % body['title'])
    print('Editing Content Status: %s' % body['stat'])
    print('Editing Content type: %s\n' % body['type'])

    if confluence.set_content() == True:
        print('should process and put content')
        result = confluence.put_content()

        if result:
            print('Put new info to Confluence Success')
        else:
            print('Put new info to Confluence Fail')

    else:
        print('content no change should not process')


def auto_update():
    print('Updating Script\n')
    path = '--git-dir=/usr/bin/machines-status/.git'
    subprocess.check_output('git ' + path +' stash',shell=True)
    subprocess.check_output('git ' + path +' pull',shell=True)

if __name__ == '__main__':
    main()
