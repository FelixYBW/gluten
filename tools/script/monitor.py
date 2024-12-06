import os
import subprocess
import sys
import time
import socket

from pathlib import Path
home = "/data/nvme1n1/binwei"

localhost = socket.gethostname()

#out = subprocess.check_output('''yarn node --list 2>&1 | grep RUNNING''',shell=True).decode('ascii').strip().split("\n")
#clients = [l.strip().split("\t")[0].split(":")[0] for l in out]

clients = [
        "monarch-dev-015-20221117-dpp-worker-dev-0a026911.ec2.pin220.com",
        "monarch-dev-015-20221117-dpp-worker-dev-0a027121.ec2.pin220.com",
        "monarch-dev-015-20221117-dpp-worker-dev-0a027c11.ec2.pin220.com"
        ]


local_profile_dir="~/profile/current_app/"

def killsar():
    for l in clients:
        out=subprocess.check_output(["ssh",l.strip(), "ps aux | grep -w sar | grep -v grep | tr -s ' ' | cut -d' ' -f2"]).decode('ascii').strip().split("\n")
        for x in out:
            subprocess.call(f'ssh {l} "kill {x} > /dev/null 2>&1"',shell=True)

def startmonitor():
    prof=local_profile_dir
#    subprocess.call(f'mkdir -p {prof}',shell=True)

    for l in clients:
        print(l,subprocess.check_output(f'''ssh {l} date''',shell=True).decode('ascii'))

    killsar()

    for l in clients:
        subprocess.call(f'mkdir -p {prof}/{l}/',shell=True)
        subprocess.call(f'ssh {l} mkdir -p {prof}/{l}/',shell=True)
        subprocess.call(f'ssh {l} "sar -o {prof}/{l}/sar.bin -r -u -d -B -n DEV 1 >/dev/null 2>&1 &"',shell=True)
    return prof

def stopmonitor(appid):

    prof=home+"/profile/"+appid+"/"
    subprocess.call(f'''mkdir -p {prof}''',shell=True)

    killsar()

    with open(f"{prof}/starttime","w") as f:
        f.write("{:d}".format(int(time.time()*1000)))

    for l in clients:
        subprocess.call(f'''ssh {l} "sar -f {local_profile_dir}/{l}/sar.bin -r > {local_profile_dir}/{l}/sar_mem.sar;sar -f {local_profile_dir}/{l}/sar.bin -u > {local_profile_dir}/{l}/sar_cpu.sar;sar -f {local_profile_dir}/{l}/sar.bin -pd > {local_profile_dir}/{l}/sar_disk.sar;sar -f {local_profile_dir}/{l}/sar.bin -n DEV > {local_profile_dir}/{l}/sar_nic.sar;sar -f {local_profile_dir}/{l}/sar.bin -B > {local_profile_dir}/{l}/sar_page.sar;"  ''',shell=True)
        if l!= socket.gethostname():
            subprocess.call(f'''scp -r {l}:{local_profile_dir}/{l} {prof}/ > /dev/null 2>&1 ''', shell=True)
        subprocess.call(f'''ssh {l} "sar -V " > {prof}/{l}/sarv.txt ''', shell=True)
        subprocess.call(f'ssh {l} "rm -rf {local_profile_dir}"',shell=True)

    subprocess.call(f'''hadoop fs -put {prof} /profile/''', shell=True)

if __name__ == '__main__':
    if sys.argv[1]=="start":
        startmonitor()
    elif sys.argv[1]=="stop":
        stopmonitor( sys.argv[2])
    elif sys.argv[1]=="kill":
        killsar()
    elif sys.argv[1]=="clean":
        for l in clients:
            subprocess.call(f'ssh {l} "rm -rf ~/profile"',shell=True)


#os.system(("ssh -o \"{}\" yuzhou@10.0.2.125 \" /home/yuzhou/PAUS/gluten/bin/run_notebook_jenkins_tpch.sh {} {} {} {} {} {}\"").format(proxy_command, date.today().strftime("%Y_%m_%d"), sys.argv[2], lastnightrun[1],lastnightrun[2], basedir, baserunid))
