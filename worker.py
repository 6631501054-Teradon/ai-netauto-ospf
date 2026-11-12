import os
import pty
import json
import time
import select
import subprocess
import requests

from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "network-config-jobs")
BACKEND_URL = "http://localhost:8000"

ROUTER_USER = os.getenv("ROUTER_USER", "admin")
ROUTER_PASS = os.getenv("ROUTER_PASS", "admin")
ROUTER_SECRET = os.getenv("ROUTER_SECRET", ROUTER_PASS)

PROCESSED_JOBS = set()

def load_devices():
    with open("devices.json", "r") as f:
        return json.load(f)

def update_backend_job(job_id: str, status: str, deploy_out: str = "", verify_out: str = ""):
    try:
        requests.post(f"{BACKEND_URL}/api/jobs/{job_id}/update", json={
            "status": status,
            "deploy_output": deploy_out,
            "verify_output": verify_out
        }, timeout=5)
    except Exception as e:
        print(f"[!] Failed to update backend for {job_id}: {e}")

def verify_approval(plan_id: str) -> bool:
    try:
        res = requests.get(f"{BACKEND_URL}/api/plans/{plan_id}", timeout=5)
        if res.status_code == 200:
            return res.json().get("approved", False)
    except Exception as e:
        print(f"[!] Approval check error: {e}")
    return False

def run_cisco_ssh(host: str, port: int, cmd_list: list) -> str:
    ssh_cmd = [
        "sshpass", "-p", ROUTER_PASS,
        "ssh", "-tt",
        "-p", str(port),
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "KexAlgorithms=+diffie-hellman-group14-sha1,diffie-hellman-group1-sha1,diffie-hellman-group-exchange-sha1",
        "-o", "HostKeyAlgorithms=+ssh-rsa",
        "-o", "PubkeyAcceptedAlgorithms=+ssh-rsa",
        "-o", "Ciphers=+aes128-cbc,3des-cbc,aes256-cbc,aes128-ctr,aes256-ctr",
        f"{ROUTER_USER}@{host}"
    ]

    master_fd, slave_fd = pty.openpty()
    proc = subprocess.Popen(
        ssh_cmd,
        stdin=slave_fd,
        stdout=slave_fd,
        stderr=slave_fd,
        close_fds=True
    )
    os.close(slave_fd)

    output = ""
    def drain(wait_sec=1.0):
        nonlocal output
        end_time = time.time() + wait_sec
        while time.time() < end_time:
            r, _, _ = select.select([master_fd], [], [], 0.2)
            if r:
                try:
                    data = os.read(master_fd, 4096).decode("utf-8", errors="ignore")
                    if not data:
                        break
                    output += data
                except OSError:
                    break

    # รอจนกระทั่ง sshpass ล็อกอินสำเร็จและได้ Prompt (# หรือ >)
    for _ in range(15):
        drain(0.5)
        if "#" in output or ">" in output:
            break

    # หากสิทธิ์ยังไม่เป็น enable (>) ให้ส่ง enable + secret
    if ">" in output and "#" not in output:
        os.write(master_fd, b"enable\r")
        drain(0.8)
        os.write(master_fd, (ROUTER_SECRET + "\r").encode())
        drain(0.8)

    # ส่งคำสั่งทีละบรรทัด
    for cmd in cmd_list:
        os.write(master_fd, (cmd + "\r").encode())
        drain(0.7)

    os.write(master_fd, b"exit\r")
    drain(1.0)

    os.close(master_fd)
    proc.terminate()
    return output

def execute_ssh_job(device_cfg: dict, commands: list):
    host = device_cfg["host"]
    port = device_cfg.get("port", 22)

    deploy_cmds = [
        "terminal length 0",
        "configure terminal",
        *commands,
        "end",
        "write memory",
        ""
    ]
    deploy_log = run_cisco_ssh(host, port, deploy_cmds)

    time.sleep(20)

    verify_cmds = [
        "terminal length 0",
        "show ip ospf neighbor",
        "show ip route ospf",
        "show ip protocols"
    ]
    verify_raw = run_cisco_ssh(host, port, verify_cmds)

    is_full = "FULL/" in verify_raw.upper()
    has_ospf_route = "via 10.1.0." in verify_raw

    return deploy_log, verify_raw.strip(), (is_full and has_ospf_route)

def main():
    print(f"[*] Starting Kafka Worker listening on '{KAFKA_TOPIC}'...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="ospf-automation-workers",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    for message in consumer:
        job = message.value
        job_id = job["job_id"]
        plan_id = job["plan_id"]
        device_id = job["device_id"]
        commands = job["commands"]

        if job_id in PROCESSED_JOBS:
            print(f"[-] Skipping duplicate job {job_id}")
            continue
        PROCESSED_JOBS.add(job_id)

        print(f"[+] Received Job {job_id} for {device_id}")

        if not verify_approval(plan_id):
            print(f"[!] Job {job_id} rejected: Plan {plan_id} is NOT approved!")
            update_backend_job(job_id, "FAILED", deploy_out="Security Block: Plan not approved by admin.")
            continue

        devices = load_devices()
        if device_id not in devices:
            update_backend_job(job_id, "FAILED", deploy_out=f"Device {device_id} not in devices.json")
            continue

        update_backend_job(job_id, "RUNNING", deploy_out="Connecting via Cisco SSH + PTY...")

        try:
            deploy_out, verify_out, verified = execute_ssh_job(devices[device_id], commands)
            final_status = "SUCCESS" if verified else "FAILED"
            if not verified:
                verify_out = "⚠️ WARNING: OSPF FULL adjacency or remote OSPF route not confirmed yet!\n\n" + verify_out
            update_backend_job(job_id, final_status, deploy_out=deploy_out, verify_out=verify_out)
            print(f"[✓] Job {job_id} finished with status: {final_status}")

        except Exception as e:
            update_backend_job(job_id, "FAILED", deploy_out=f"SSH/Execution Error: {str(e)}")

if __name__ == "__main__":
    main()
