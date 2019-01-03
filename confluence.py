#v1.1.9
import re
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
import configs_collector

class Confluence(object):
    """docstring for Confluence."""
    def __init__(self, unravel_base_url, atlassian_base_url, credentials, alias_name=None):
        self.unravel_base = unravel_base_url
        self.al_base = atlassian_base_url
        self.get_content_body_url = self.al_base + '?expand=body.storage'
        self.credentials = credentials
        self.unravel_version_url = configs_collector.get_unravel_ver()
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

    # Get Main Body Content from Confluence
    def get_content_body(self):
        res = requests.get(self.get_content_body_url, headers=self.headers)
        content = json.loads(res.text)

        self.content_title = content['title']
        self.content_type = content['type']
        self.content_stat = content['status']
        hosts = self.get_host()
        self.body = content['body']['storage']['value']

        return {'title': self.content_title, 'type': self.content_type, 'stat': self.content_stat, 'hosts': hosts}

    # Get Confluence's Content Current Version Number
    def get_content_ver(self):
        al_ver_req = requests.get(self.al_base, headers=self.headers)

        self.content_ver = str(json.loads(al_ver_req.text)['version']['number']+1)

        return self.content_ver

    # Get Current Hostname and ip address
    def get_host(self):
            self.server_name = subprocess.check_output(['hostname', '-s']).split('\n')[0]
            self.ip_addr = subprocess.check_output(['hostname', '-i']).split('\n')[0]
            return(self.server_name, self.ip_addr)

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

    # Set New Content that will send back to Confluence
    def set_content(self):
        should_process = False
        soup = BeautifulSoup(self.body, "html.parser")

        try:
            if self.al_base == 'https://unraveldata.atlassian.net/wiki/rest/api/content/502628605':  # Test Cluster Wiki Page
                tag = soup.find(text=self.server_name).find_next('span').find(text=re.compile('4.[0-9].[0-9].[0-9](.[0-9]b[0-9]{1,4})?')).find_parent()
                print(str(tag))
            else:
                tag = soup.find(text=self.server_name).find_parent('td').find_next('td').find_next('td').find_next('td').find_next('td')
                print(str(tag))
        except Exception as e:
            print('No server name Found')
            print('Now Looking for IP address instead\n')
            try:
                if self.al_base == 'https://unraveldata.atlassian.net/wiki/rest/api/content/502628605':
                    tag = soup.find(text=self.ip_addr).find_next('span').find(text=re.compile('4.[0-9].[0-9].[0-9](.[0-9]b[0-9]{1,4})?')).find_parent()
                    print(str(tag))
                else:
                    tag = soup.find(text=self.ip_addr).find_parent('td').find_previous('td')
                    print(str(tag))
            except:
                print('No IP address Found')
                print('Now Looking for alias name instead\n')
                try:
                    if self.al_base == 'https://unraveldata.atlassian.net/wiki/rest/api/content/502628605':
                        tag = soup.find_all(text=re.compile(self.alias_name+'.*'))[-1].find_next('span').find(text=re.compile('4.[0-9].[0-9].[0-9](.[0-9]b[0-9]{1,4})?')).find_parent()
                        print(str(tag))
                    else:
                        tag = soup.find(text=self.alias_name).find_parent('td').find_next('td').find_next('td').find_next('td').find_next('td')
                        print(str(tag))
                except:
                    print('No alias name Found')
                    return should_process

        if tag.string and tag.string != self.unravel_version:
            tag.string = self.unravel_version
            should_process = True
        elif not tag.string and self.unravel_version:
            tag.string = self.unravel_version
            should_process = True

        self.new_content = str(soup)
        return should_process

    # Get Unravel Version Number From version.txt
    def unravel_ver(self):
        try:
            res = requests.get(self.unravel_version_url)

            if res.status_code == 200:
                unravel_version = re.search('[0-9]{1,2}.[0-9]{1,2}.[0-9]{,2}.[0-9a-zA-Z]{,6}', res.text)
                self.unravel_version = str(unravel_version.group(0))
                return self.unravel_version
            else:
                print('Unknowm Unravel Version')
                self.unravel_version = '4.0.0.0'
                return self.unravel_version
        except:
            self.unravel_version = '4.0.0.0'
            return self.unravel_version
