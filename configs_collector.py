#!/usr/bin/python
"""
Script to get configurations from CM/AM
"""
import re
import os
import requests
from subprocess import PIPE, Popen


def get_cluster_type():
    try:
        hadoop_version_string = Popen("hadoop version", stdout=PIPE, shell=True).communicate()[0]
        if re.search('CDH|cdh|cloudera', hadoop_version_string):
            cluster_type = 'CDH'
        elif re.search('HDP|hdp|ambari', hadoop_version_string):
            cluster_type = 'HDP'
        elif re.search('mapr|MAPR', hadoop_version_string):
            cluster_type = 'MAPR'
        else:
            cluster_type = 'UNKNOWN'
    except:
        cluster_type = 'UNKNOWN'
    return cluster_type


class CMMetrics:
    def __init__(self, host, port, username, password, api_ver=11, protocol='http'):
        self.host = host
        self.port = port
        self.api_ver = api_ver
        self.protocol = protocol
        self.session = requests.session()
        self.session.auth = (username, password)
        # self.session.verify = False
        self.api_url = "{0}://{1}:{2}/api/v{3}".format(protocol, host, port, api_ver)
        self.cluster_name = self._get_req("{0}/{1}".format(self.api_url, 'clusters')).json()['items'][0]['displayName']
        self.cluster_ver = self._get_req("{0}/clusters/{1}".format(self.api_url, self.cluster_name)).json()["fullVersion"]
        self.services_api_url = "{0}/clusters/{1}/services".format(self.api_url, self.cluster_name)
        self.host_dict = self._get_host_dict()

    def _get_req(self, req_url):
        try:
            req = self.session.get(req_url)
        except requests.exceptions.SSLError:
            print('Invalid SSL Cert found, turning off certificate verification')
            self.session.verify = False
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            req = self.session.get(req_url)
        if req.status_code == 403:
            raise urllib3.exceptions.ConnectionError('403 Missing authentication token')
            exit(1)
        elif req.status_code == 404:
            # raise urllib3.exceptions.ConnectionError('URL: {} not found'.format(req_url))
            pass
        return req

    def _get_host_dict(self):
        hosts_dict = {}
        for item in self._get_req("{}/{}".format(self.api_url, 'hosts')).json()['items']:
            hosts_dict[item['hostId']] = item['hostname']
        return hosts_dict
        # except Exception as e:
        #     print(e)
        #     raise ValueError('Failed to get cluster hosts list')

    def get_cm_active_namenode(self):
        cm_active_namenode = {}
        req = self.get_service_roles('hdfs')
        if req:
            for item in req:
                if item['type'] == 'NAMENODE' and item['haStatus'] == 'ACTIVE':
                    cm_active_namenode['active_namenode'] = self.host_dict[item['hostRef']['hostId']]
                    for config in self.get_role_config('hdfs', item['name']):
                        if config['name'] == "namenode_port":
                            cm_active_namenode['namenode_port'] = config.get("value", 8020)
                    break
        return cm_active_namenode

    def get_cm_hive_metastore(self):
        cm_hive_metastore = {'metastore_hosts': []}
        req = self.get_service_roles('hive')
        if req:
            for item in req:
                if item['type'] == 'HIVEMETASTORE':
                    cm_hive_metastore['metastore_hosts'].append(self.host_dict[item['hostRef']['hostId']])
                    for config in self.get_role_config('hive', item['name']):
                        if config['name'] == "hiveserver2_webui_port":
                            cm_hive_metastore['hs2_webui_port'] = config.get("value", 10002)
        return cm_hive_metastore

    def get_impala_daemon(self):
        cm_impala_daemon = {'impala_hosts': []}
        req = self.get_service_roles('impala')
        if req:
            for item in req:
                if item['type'] == 'IMPALAD':
                    cm_impala_daemon['impala_hosts'].append(self.host_dict[item['hostRef']['hostId']])
                    for config in self.get_role_config('impala', item['name']):
                        if config['name'] == "beeswax_port":
                            cm_impala_daemon['beeswax_port'] = config.get("value", 21000)
        return cm_impala_daemon

    def get_cm_hiveserver2(self):
        cm_hiveserver2 = {'hive_server2_hosts': []}
        req = self.get_service_roles('hive')
        if req:
            for item in req:
                if item['type'] == 'HIVESERVER2':
                    cm_hiveserver2['hive_server2_hosts'].append(self.host_dict[item['hostRef']['hostId']])
                    for config in self.get_role_config('hive', item['name']):
                        if config['name'] == "hs2_thrift_address_port":
                            cm_hiveserver2['hs2_thrift_port'] = config.get("value", 10000)
        return cm_hiveserver2

    def get_cm_kafka_brokers(self):
        cm_brokers = {'broker_hosts': []}
        req = self.get_service_roles('kafka')
        if req:
            for item in req:
                if item['type'] == 'KAFKA_BROKER':
                    cm_brokers['broker_hosts'].append(self.host_dict[item['hostRef']['hostId']])
                    for config in self.get_role_config('kafka', item['name']):
                        if config['name'] == "jmx_port":
                            cm_brokers['broker_jmx_port'] = config.get("value", 9393)
                        elif config['name'] == "port":
                            cm_brokers['kafka_port'] = config.get("value", 9092)
        return cm_brokers

    def get_cm_oozie_server(self):
        cm_oozie_server = {}
        req = self.get_service_roles('oozie')
        if req:
            for item in req:
                if item['type'] == 'OOZIE_SERVER':
                    cm_oozie_server['oozie_server_host'] = self.host_dict[item['hostRef']['hostId']]
                    for config in self.get_role_config('oozie', item['name']):
                        if config['name'] == "oozie_http_port":
                            cm_oozie_server['oozie_http_port'] = config.get("value", 11000)
        return cm_oozie_server

    def get_cm_zookeeper(self):
        cm_zookeeper = {'zk_server_hosts': []}
        req = self.get_service_roles('zookeeper')
        if req:
            for item in req:
                if item['type'] == 'SERVER':
                    cm_zookeeper['zk_server_hosts'].append(self.host_dict[item['hostRef']['hostId']])
                    for config in self.get_role_config('zookeeper', item['name']):
                        if config['name'] == "clientPort":
                            cm_zookeeper['zk_client_port'] = config.get('value', 2181)
        return cm_zookeeper

    def get_role_config(self, service_name, role_name):
        req = self._get_req("{}/{}/roles/{}/config?view=full".format(self.services_api_url, service_name, role_name))
        if req.status_code != 200:
            print("get role config request return: " + str(req.status_code))
        return req.json()['items']

    def get_service_roles(self, service_name):
        req = self._get_req("{}/{}/roles".format(self.services_api_url, service_name))
        if req.status_code == 404:
            print("service {} not found".format(service_name))
        elif req.status_code != 200:
            print("get service role request return: " + str(req.status_code))
        if req.status_code != 200:
            return None
        return req.json()['items']


class AMMetrics:
    def __init__(self, host, port, username, password, api_ver=1, protocol='http'):
        self.host = host
        self.port = port
        self.api_ver = api_ver
        self.protocol = protocol
        self.session = requests.session()
        self.session.auth = (username, password)
        # self.session.verify = False
        self.api_url = "{0}://{1}:{2}/api/v{3}".format(protocol, host, port, api_ver)
        cluster_infos = self._get_req("{0}/{1}".format(self.api_url, 'clusters')).json()['items'][0]['Clusters']
        self.cluster_name = cluster_infos['cluster_name']
        self.cluster_ver = cluster_infos["version"]
        self.cur_config_tag = self._get_req("{0}/{1}".format(self.api_url, 'clusters?fields=Clusters/desired_configs')).json()['items'][0]['Clusters']['desired_configs']
        self.api_with_name = "{0}/clusters/{1}".format(self.api_url, self.cluster_name)
        self.configs_base_url = "{0}/{1}".format(self.api_url, 'clusters/' + self.cluster_name + '/configurations')
        # self.host_dict = self._get_host_dict()

    def _get_req(self, req_url):
        try:
            req = self.session.get(req_url)
        except requests.exceptions.SSLError:
            print('Invalid SSL Cert found, turning off certificate verification')
            self.session.verify = False
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            req = self.session.get(req_url)
        if req.status_code == 403:
            raise urllib3.exceptions.ConnectionError('403 Missing authentication token')
            exit(1)
        elif req.status_code == 404:
            # raise urllib3.exceptions.ConnectionError('URL: {} not found'.format(req_url))
            pass
        return req

    def get_am_active_namenode(self):
        am_active_namenode = {}
        req = self.get_host_components('NAMENODE')
        for item in req['items']:
            ha_state = self._get_req(item['href']).json()['HostRoles'].get('ha_state', None)
            if ha_state == 'ACTIVE' or None:
                am_active_namenode['active_namenode'] = item['HostRoles']['host_name']
                am_active_namenode['namenode_port'] = self.get_configs('hdfs-site')['items'][0]['properties'].get('fs.defaultFS', 8020)
        return am_active_namenode

    def get_am_ats(self):
        am_ats = {}
        am_ats['ats_web_address'] = self.get_configs('yarn-site')['items'][0]['properties'].get(
            'yarn.timeline-service.webapp.address', '')
        am_ats['ats_web_https_address'] = self.get_configs('yarn-site')['items'][0]['properties'].get(
            'yarn.timeline-service.webapp.https.address', '')
        return am_ats

    def get_am_hive_metastore(self):
        am_hive_metastore = {'metastore_hosts': []}
        req = self.get_host_components('HIVE_METASTORE')
        for item in req['items']:
            am_hive_metastore['metastore_hosts'].append(item['HostRoles']['host_name'])
        return am_hive_metastore

    def get_am_hiveserver2(self):
        am_hs2 = {'hive_server2_hosts': []}
        req = self.get_host_components('HIVE_SERVER')
        for item in req['items']:
            am_hs2['hive_server2_hosts'].append(item['HostRoles']['host_name'])
        am_hs2['hs2_thrift_port'] = self.get_configs('hive-site')['items'][0]['properties'].get('hive.server2.thrift.port', 10000)
        return am_hs2

    def get_am_oozie_server(self):
        am_oozie = {}
        req = self.get_host_components('OOZIE_SERVER')
        for item in req['items']:
            am_oozie['oozie_server_host'] = item['HostRoles']['host_name']
        oozie_port = self.get_configs('oozie-site')['items'][0]['properties'].get('oozie.base.url', 11000).split(':')[-1].split('/')[0]
        am_oozie['oozie_http_port'] = oozie_port
        return am_oozie

    def get_am_zookeeper(self):
        am_zk = {'zk_server_hosts': []}
        req = self.get_host_components('ZOOKEEPER_SERVER')
        for item in req['items']:
            am_zk['zk_server_hosts'].append(item['HostRoles']['host_name'])
        am_zk['zk_client_port'] = self.get_configs('zoo.cfg')['items'][0]['properties'].get('clientPort', 2181)
        return am_zk

    def get_am_kafka_broker(self):
        am_kafka = {'broker_hosts': []}
        req = self.get_host_components('KAFKA_BROKER')
        for item in req['items']:
            am_kafka['broker_hosts'].append(item['HostRoles']['host_name'])
        am_kafka['broker_port'] = self.get_configs('kafka-broker')['items'][0]['properties'].get('port', 9092)
        kafka_env = self.get_configs('kafka-env')['items'][0]['properties']['content']
        search_jmx = re.search("JMX_PORT=(.*)", kafka_env)
        if search_jmx:
            am_kafka['broker_jmx_port'] = search_jmx.group(1)
        return am_kafka

    def get_host_components(self, component_name):
        req = self._get_req("{}/{}".format(self.api_with_name, 'host_components?HostRoles/component_name=%s' % component_name))
        return req.json()

    def get_configs(self, conf_type):
        conf_tag = self.cur_config_tag[conf_type]['tag']
        req = self._get_req("{0}?{1}".format(self.configs_base_url, 'type={0}&tag={1}'.format(conf_type, conf_tag)))
        return req.json()


def pretty_print(dict_in):
    for key, val in dict_in.iteritems():
        if type(val) is list:
            print("{0}:\n\t{1}".format(key, '\n\t'.join(val)))
        else:
            print("{0}: {1}".format(key, val))
    print('')


def get_server():
    server_host = None
    if cluster_type == "CDH":
        cloudera_agent_conf_path = '/etc/cloudera-scm-agent/config.ini'
        with open(cloudera_agent_conf_path, "r") as f:
            server_host = re.search('server_host=.*', f.read()).group(0).split('=')[1]
    elif cluster_type == "HDP":
        ambari_agent_conf_path = '/etc/ambari-agent/conf/ambari-agent.ini'
        with open(ambari_agent_conf_path, "r") as f:
            server_host = re.search('hostname=.*', f.read()).group(0).split('=')[1]
    elif cluster_type == "MAPR":
        pass
    return server_host

def get_unravel_ver():
    """
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


cluster_type = get_cluster_type()

if __name__ == '__main__':
    if cluster_type == "CDH":
        cm_host = get_server()
        try:
            cm_metrics = CMMetrics(cm_host, 7180, 'admin', 'admin')
        except:
            cm_metrics = CMMetrics(cm_host, 7183, 'admin', 'admin', protocol='https')
        print("CDH Version: {0}".format(cm_metrics.cluster_ver))
        pretty_print(cm_metrics.get_cm_active_namenode())
        pretty_print(cm_metrics.get_cm_hive_metastore())
        pretty_print(cm_metrics.get_cm_hiveserver2())
        pretty_print(cm_metrics.get_cm_zookeeper())
        pretty_print(cm_metrics.get_cm_oozie_server())
        pretty_print(cm_metrics.get_impala_daemon())
        pretty_print(cm_metrics.get_cm_kafka_brokers())
    elif cluster_type == "HDP":
        am_host = get_server()
        try:
            am_metrics = AMMetrics(am_host, 8080, 'admin', 'admin')
        except:
            am_metrics = AMMetrics(am_host, 8443, 'admin', 'admin', protocol='https')
        print("HDP Version: {0}".format(am_metrics.cluster_ver))
        pretty_print(am_metrics.get_am_active_namenode())
        pretty_print(am_metrics.get_am_hive_metastore())
        pretty_print(am_metrics.get_am_hiveserver2())
        pretty_print(am_metrics.get_am_zookeeper())
        pretty_print(am_metrics.get_am_oozie_server())
        pretty_print(am_metrics.get_am_kafka_broker())
        pretty_print(am_metrics.get_am_ats())
    elif cluster_type == "MAPR":
        pass
    else:
        print("Unknown cluster type exiting")
        exit(1)