#v1.1.9
import os
import subprocess
try:
    import requests
except:
    subprocess.call(['sudo', 'yum', 'install', 'epel-release', '-y'])
    subprocess.call(['sudo', 'yum', 'install', 'python-pip', '-y'])
    subprocess.call(['sudo', 'pip', 'install', 'requests'])
    import requests
import json
try:
    from bs4 import BeautifulSoup
except:
    subprocess.call(['sudo', 'yum', 'install', 'python-pip', '-y'])
    subprocess.call(['sudo', 'pip', 'install', 'BeautifulSoup4'])
    from bs4 import BeautifulSoup
import re


class Confluence(object):
    """docstring for updating Confluence Wiki Page"""
    def __init__(self, unravel_base_url, atlassian_base_url, credentials, alias_name=None):
        self.unravel_base = unravel_base_url
        self.al_base = atlassian_base_url
        self.get_content_body_url = self.al_base + '?expand=body.storage'
        self.credentials = credentials
        self.unravel_version = self.unravel_ver()
        self.headers = {'Authorization': 'Basic %s' % credentials}
        self.body = None
        self.content_ver = None
        self.content_title = None
        self.content_stat = None
        self.content_type = None
        self.content_stat = None
        self.alias_name = alias_name
        self.server_name = None
        self.ip_addr = None

    def get_content_body(self):
        """
        Get Main Body Content from Confluence API
        :return: wiki page: title, type, stat, hosts
        :rtype: dict
        """
        res = requests.get(self.get_content_body_url, headers=self.headers)
        content = json.loads(res.text)

        self.content_title = content['title']
        self.content_type = content['type']
        self.content_stat = content['status']
        hosts = self.get_host()
        self.body = content['body']['storage']['value']

        return {'title': self.content_title, 'type': self.content_type, 'stat': self.content_stat, 'hosts': hosts}

    @property
    def get_content_ver(self):
        """
        Get Confluence's Content Current Version Number
        :return: Current Confluence Page Version Number
        :rtype: int
        """
        al_ver_req = requests.get(self.al_base, headers=self.headers)
        if al_ver_req.status_code != 200:
            print(al_ver_req.content)
            exit(1)
        self.content_ver = str(json.loads(al_ver_req.text)['version']['number']+1)

        return self.content_ver

    def get_host(self):
        """
        Get Current Hostname and ip address
        :return: server_name, server ip address
        :rtype: str
        """
        self.server_name = subprocess.check_output(['hostname', '-s']).split('\n')[0]
        self.ip_addr = subprocess.check_output(['hostname', '-i']).split('\n')[0]
        return self.server_name, self.ip_addr

    # Save content back to Confluence
    def put_content(self):
        data = {
                "title": self.content_title,
                "type": self.content_type,
                "status": self.content_stat,
                "version": {
                            "number": self.content_ver,
                            "minorEdit": True
                            },
                "body": {
                         "storage": {
                                      "value": self.new_content,
                                          "representation": "storage"
                                     }
                        }
                }

        data = json.loads(json.dumps(data))
        self.headers['content-type'] = 'application/json'
        res = requests.put(self.al_base, headers=self.headers, json=data)
        if res.status_code == 200:
            return True
        else:
            return res.status_code

    def set_content(self):
        """ Set New Content that will send back to Confluence """
        should_process = False  # whether wiki page need to be updated
        soup = BeautifulSoup(self.body, "html.parser")

        try:
            # Test Cluster Wiki Page
            tag = soup.find(text=self.server_name).find_next('span').find(text=re.compile('4.[0-9].[0-9].[0-9](.[0-9]b[0-9]{1,4})?|UNKNOWN')).find_parent()
            print(str(tag))
        except Exception as e:
            print('No server name Found')
            print('Now Looking for IP address instead\n')
            try:
                tag = soup.find(text=self.ip_addr).find_next('span').find(text=re.compile('4.[0-9].[0-9].[0-9](.[0-9]b[0-9]{1,4})?')).find_parent()
                print(str(tag))
            except:
                print('No IP address Found')
                print('Now Looking for alias name instead\n')
                try:
                    tag = soup.find_all(text=re.compile(self.alias_name+'.*'))[-1].find_next('span').find(text=re.compile('4.[0-9].[0-9].[0-9](.[0-9]b[0-9]{1,4})?|UNKNOWN')).find_parent()
                    print(str(tag))
                except:
                    print('No alias name Found')
                    return should_process

        # compare unravel version in wiki page with local
        if tag.string and tag.string != self.unravel_version:
            tag.string = self.unravel_version
            should_process = True
        elif not tag.string and self.unravel_version:
            tag.string = self.unravel_version
            should_process = True

        # Create new wiki page content for update
        self.new_content = str(soup)
        return should_process

    @staticmethod
    def unravel_ver():
        """
        Get Unravel Version Number From version.txt
        :return: installed unravel version number
        """
        unravel_version_path = "/usr/local/unravel/ngui/www/version.txt"
        unravel_ver = "UNKNOWN"
        if os.path.exists(unravel_version_path):
            with open(unravel_version_path, 'r') as f:
                version_file = f.read()
                f.close()
            if re.search('4\.[0-9]\.[0-9].*', version_file):
                return re.search('4\.[0-9]\.[0-9].*', version_file).group(0)
        return unravel_ver
