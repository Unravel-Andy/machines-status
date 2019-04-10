import re
import subprocess
import datetime
import platform
from subprocess import Popen, PIPE
import configs_collector as CC
import mongodb_connector


def get_host():
    server_name = subprocess.check_output(['hostname', '-f']).split('\n')[0]
    ip_addr = subprocess.check_output(['hostname', '-i']).split('\n')[0]
    return server_name, ip_addr


def get_alias_name(hostname):
    alias_name = "UNKNOWN"
    try:
        popen_prop = Popen("bash -c '. /etc/profile && all-node-list | grep {0}'".format(hostname), shell=True, stdout=PIPE)
        result = popen_prop.communicate()
        alias_name = re.split("\s|\.", result[0])[0]
        return alias_name
    except:
        return alias_name


def send_to_db(alias_name=None):
    host_name, host_ip = get_host()
    if not alias_name:
        alias_name = get_alias_name(host_name)
    db_connector = mongodb_connector.DBConnector(db_host="172.66.1.211")
    cur_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if CC.cluster_type == "CDH":
        cm_host = CC.get_server()
        try:
            cm_metrics = CC.CMMetrics(cm_host, 7180, 'admin', 'admin')
        except:
            cm_metrics = CC.CMMetrics(cm_host, 7183, 'admin', 'admin', protocol='https')
        query = {"hostname": host_name}
        new_data = {"alias": alias_name,
                    "cluster_type": "CDH",
                    "cluster_version": cm_metrics.cluster_ver,
                    "namenode": cm_metrics.get_cm_active_namenode(),
                    "hive_metastore": cm_metrics.get_cm_hive_metastore(),
                    "hiveserver2": cm_metrics.get_cm_hiveserver2(),
                    "zookeeper": cm_metrics.get_cm_zookeeper(),
                    "impala": cm_metrics.get_impala_daemon(),
                    "oozie_server": cm_metrics.get_cm_oozie_server(),
                    "kafka_broker": cm_metrics.get_cm_kafka_brokers(),
                    "unravel_version": CC.get_unravel_ver(),
                    "update_time": cur_time,
                    "unravel_db_type": CC.get_unravel_db_type(),
                    "security type": cm_metrics.get_secure_type()}
    elif CC.cluster_type == "HDP":
        am_host = CC.get_server()
        try:
            am_metrics = CC.AMMetrics(am_host, 8080, 'admin', 'admin')
        except:
            am_metrics = CC.AMMetrics(am_host, 8443, 'admin', 'admin', protocol='https')
        query = {"hostname": host_name}
        new_data = {"alias": alias_name,
                    "cluster_type": "HDP",
                    "cluster_version": am_metrics.cluster_ver,
                    "namenode": am_metrics.get_am_active_namenode(),
                    "hive_metastore": am_metrics.get_am_hive_metastore(),
                    "hiveserver2": am_metrics.get_am_hiveserver2(),
                    "zookeeper": am_metrics.get_am_zookeeper(),
                    "ats": am_metrics.get_am_ats(),
                    "oozie_server": am_metrics.get_am_oozie_server(),
                    "kafka_broker": am_metrics.get_am_kafka_broker(),
                    "unravel_version": CC.get_unravel_ver(),
                    "update_time": cur_time,
                    "unravel_db_type": CC.get_unravel_db_type(),
                    "security type": am_metrics.get_secure_type()}
    elif CC.cluster_type == "MAPR":
        mapr_metrics = CC.MAPRMetrics()
        query = {"hostname": host_name}
        new_data = {"alias": alias_name,
                    "cluster_type": "MAPR",
                    "cluster_version": mapr_metrics.get_mapr_version(),
                    "unravel_version": CC.get_unravel_ver(),
                    "update_time": cur_time,
                    "unravel_db_type": CC.get_unravel_db_type()}
    else:
        print("Cluster type {0} not supported".format(CC.cluster_type))
        exit(0)
    new_data = {"os": " ".join(platform.linux_distribution())}
    db_connector.update(query, new_data)
