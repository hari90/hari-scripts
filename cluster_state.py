from time import sleep
import os
import subprocess
import asyncio
import threading


YBLogo = """
    ╭--------------------------------------------╮
    |ＹｕｇａｂｙｔｅＤＢ ＋ ＤｏｃｕｍｅｎｔＤＢ|
    ╰--------------------------------------------╯

"""

White = "\033[0m"
Green = "\033[92m"
Blue = "\033[94m"
Cyan = "\033[96m"
Bold = '\033[1m'
Box = "▮"

GreenBox = f"{Green}{Box}{White}"
BlueBox = f"{Blue}{Box}{White}"

is_prod = "dev-server" not in os.uname().nodename

if is_prod:
    YsqlSh = ["/home/yugabyte/tserver/bin/ysqlsh", "-h", "10.12.16.44", "-t", "-c"]
else:
    YsqlSh = ["./bin/ysqlsh","-t", "-c"]

if is_prod:
    YbAdmin = ["/home/yugabyte/master/bin/yb-admin", "--master_addresses", "10.12.16.44:7100,10.12.16.46:7100,10.12.16.47:7100", "--certs_dir_name", "/home/yugabyte/yugabyte-tls-config/"]
else:
    YbAdmin = ["./build/latest/bin/yb-admin", "--master_addresses", "127.0.0.1:7100,127.0.0.2:7100,127.0.0.3:7100"]

def execute_command(command):
    try:
        result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True
            )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        raise Exception(f"Error executing command: {e.stderr}")

def execute_command_and_get_table_output(command, split_on='|'):
    result = execute_command(command)
    result = result.split('\n')
    result = [line for line in result if line.strip()]  # Remove empty lines
    result = [' '.join(line.split()).replace(split_on * 2, split_on) for line in result]  # Replace multiple contiguous split_on with one
    result = [line.split(split_on) for line in result]
    result = [list(map(str.strip, line)) for line in result]
    return result

def get_collection_state():
    sql_cmd = """select collection_name AS collection, to_char(reltuples, '999999999') AS documents,
            (select num_tablets from yb_table_properties(oid)) AS tablets,
            (select pg_size_pretty(pg_table_size(oid))) AS size
            from documentdb_api_catalog.collections
            join pg_class on relname = 'documents_'||CAST(collection_id AS text)
            where collection_name != 'system.dbSentinel' order by collection_name"""
    result = execute_command_and_get_table_output(YsqlSh+[sql_cmd], split_on='|')
    
    return result

def get_host_tablet_count(host_uuid: str):
    tablets = execute_command_and_get_table_output(YbAdmin + ["list_tablets_for_tablet_server", host_uuid], split_on=' ')
    leaders = 0
    followers = 0
    for tablet in tablets:
        if tablet[3] == 'RUNNING':
            if tablet[2] == '0':
                followers += 1
            else:
                leaders += 1
    return leaders, followers

def get_hosts_state():
    hosts = execute_command_and_get_table_output(YbAdmin + ["list_all_tablet_servers"], split_on=' ')
    
    idx = 1
    result = []
    hosts.sort(key=lambda x: int(x[6]) if x[6].isdigit() else 0, reverse=True)
    for host in hosts:
        if host[3] == 'ALIVE':
            host_uuid = host[0]
            try:
                leaders, followers = get_host_tablet_count(host_uuid)
                result.append({'host': f'host{idx}', 'leaders': leaders, 'followers': followers})
                idx += 1
            except:
                continue
    return result

def print_cluster_state():
    collectionState =  get_collection_state()
    hostsState = get_hosts_state()

    os.system('cls' if os.name == 'nt' else 'clear')
    print(f"{Cyan}{Bold}{YBLogo}{White}")

    print(f"\n{Bold}{'Collection':<20}{'Documents':>12}{'Tablets':>12}{'Size':>13}{White}")
    print("-" * 58)
    for collection in collectionState:
        print(f"{collection[0]:<20}{collection[1]:>12} {collection[2]:>12}{collection[3]:>13}")

    print(f"\n\n{Bold}{'Host':<10}Tablets (Leaders/Total){White}")
    print("-" * 60)
    for host in hostsState:
        print(f"{host['host']:<10}{GreenBox*(int(host['leaders']/2))}{BlueBox*(int(host['followers']/2))} ({host['leaders']}/{host['leaders']+host['followers']})")


def run_loop():
    while True:
        try:
            print_cluster_state()
            sleep(0.5)
        except:
            pass

if __name__ == '__main__':
    # print(get_hosts_state())
    # print(get_collection_state())
    run_loop()
