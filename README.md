# AI-Driven OSPF Network Automation Pipeline

Automated Cisco IOS OSPF configuration and verification pipeline powered by LLM plan generation, human-in-the-loop approval, Apache Kafka message queue, and PTY-based SSH automation in GNS3.

## Architecture
1. **AI Plan Generator (FastAPI + LLM):** Generates Cisco IOS OSPF configuration commands from network topology intent.
2. **Human-in-the-Loop Approval:** Requires explicit admin approval before queuing any configuration job.
3. **Kafka Message Broker:** Decouples job submission (`network-config-jobs`) from device execution.
4. **Legacy Cisco SSH/PTY Worker:** Handles legacy SSH ciphers/KEX and interactive prompts (`sshpass` + `pty`) to deploy and save configurations (`write memory`).
5. **Automated Verification:** Validates OSPF `FULL` neighbor adjacency and remote OSPF routes (`show ip route ospf`).

## Quick Start
1. Copy `.env.example` to `.env` and fill in your credentials.
2. Install dependencies: `pip install fastapi uvicorn kafka-python python-dotenv requests`
3. Start FastAPI backend: `uvicorn app:app --host 0.0.0.0 --port 8000`
4. Start Kafka worker: `python worker.py`
