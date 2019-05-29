# machines-status

### Machines Status Consists of two parts
Part 1. This script is running in every Unravel Nodes in Colo and LabCollect Unravel Version, update Test-Cluster Wiki Page and send the information to MongoDB located in colo2jumphost

Part 2.
There are playbooks and scripts running by a cron job every 15 mins and check diskspace on every machines in inventory then send out slack message to #machinesstatus Channel if disk threshold reach


This script will update current Unravel Version in Confluence Test-Cluster wiki Page
https://unraveldata.atlassian.net/wiki/spaces/EN/pages/502628605/Test+Clusters


Example:
```bash
python run.py --username <Confluence Login Username> --password <Conluence API Token> --alias <DNS Alias Name, i.e QAHDP26B>
```
