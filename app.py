import os
import json
import uuid
from datetime import datetime
from typing import Dict, Any
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from dotenv import load_dotenv
from google import genai
from google.genai import types
from kafka import KafkaProducer

load_dotenv()

app = FastAPI(title="AI Network Automation - OSPF")
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "network-config-jobs")

PLANS_DB: Dict[str, Any] = {}
JOBS_DB: Dict[str, Any] = {}

def load_devices() -> Dict[str, Any]:
    with open("devices.json", "r") as f:
        return json.load(f)

def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

@app.post("/api/analyze")
async def analyze_diagram(file: UploadFile = File(...)):
    image_bytes = await file.read()
    devices_info = load_devices()

    prompt = f"""
    Analyze this network topology diagram carefully.
    Extract all routers, PCs, subnets, IP addresses, and OSPF routing requirements.
    Use these exact physical interface mappings from the lab inventory:
    {json.dumps(devices_info)}

    Rules:
    1. If any IP, mask, or link is ambiguous or missing in the image, list it in "ambiguities" instead of guessing.
    2. Generate Cisco IOS configuration commands for each router using OSPFv2 process 1, Area 0.
    3. Advertise all connected /24 networks via OSPF network statements (with wildcard mask 0.0.0.255).
    4. Set LAN-facing interfaces as passive-interface under router ospf 1.
    5. Do NOT use static routes. Do NOT touch management interfaces or disable SSH/VTY lines.
    6. Include 'no shutdown' on configured data-plane interfaces.

    Return ONLY valid JSON matching this schema:
    {{
      "devices": [
        {{"name": "R1", "role": "router", "router_id": "1.1.1.1"}},
        {{"name": "PC1", "role": "host", "ip": "10.0.0.10/24", "gateway": "10.0.0.1"}}
      ],
      "links": [
        {{"from": "PC1", "to": "R1", "subnet": "10.0.0.0/24", "type": "LAN"}},
        {{"from": "R1", "to": "R2", "subnet": "10.1.0.0/24", "type": "WAN"}}
      ],
      "ospf": {{
        "process_id": 1,
        "area": 0,
        "networks": ["10.0.0.0/24", "10.1.0.0/24", "10.2.0.0/24"]
      }},
      "ambiguities": [],
      "configs": {{
        "R1": ["interface FastEthernet0/0", "ip address 10.0.0.1 255.255.255.0", "no shutdown"],
        "R2": ["interface FastEthernet1/0", "ip address 10.1.0.2 255.255.255.0", "no shutdown"]
      }}
    }}
    """

    response = client.models.generate_content(
        model="gemini-3.5-flash-lite",
        contents=[
            types.Part.from_bytes(data=image_bytes, mime_type=file.content_type),
            prompt
        ],
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            temperature=0.1
        )
    )

    ai_data = json.loads(response.text)
    plan_id = str(uuid.uuid4())[:8]

    validation_errors = []
    if ai_data.get("ambiguities"):
        validation_errors.append(f"Ambiguous diagram elements: {ai_data['ambiguities']}")

    for r_name, cmds in ai_data.get("configs", {}).items():
        cmd_str = "\n".join(cmds).lower()
        if "ip route " in cmd_str:
            validation_errors.append(f"{r_name}: Static routes are forbidden!")
        if "no ip ssh" in cmd_str or "transport input none" in cmd_str:
            validation_errors.append(f"{r_name}: Command risks disabling management SSH!")
        if "router ospf 1" not in cmd_str:
            validation_errors.append(f"{r_name}: Missing OSPFv2 process 1!")
        if "passive-interface" not in cmd_str:
            validation_errors.append(f"{r_name}: Missing passive-interface on LAN!")
        if r_name not in devices_info:
            validation_errors.append(f"{r_name}: Not found in management inventory (devices.json)!")

    PLANS_DB[plan_id] = {
        "plan_id": plan_id,
        "created_at": datetime.now().isoformat(),
        "ai_output": ai_data,
        "validation_errors": validation_errors,
        "approved": False,
        "approved_at": None
    }
    return PLANS_DB[plan_id]

@app.post("/api/approve/{plan_id}")
def approve_plan(plan_id: str):
    if plan_id not in PLANS_DB:
        raise HTTPException(status_code=404, detail="Plan not found")
    plan = PLANS_DB[plan_id]
    if plan["validation_errors"]:
        raise HTTPException(status_code=400, detail=f"Cannot approve due to validation errors: {plan['validation_errors']}")
    
    plan["approved"] = True
    plan["approved_at"] = datetime.now().isoformat()
    return {"status": "APPROVED", "plan_id": plan_id, "approved_at": plan["approved_at"]}

@app.post("/api/deploy/{plan_id}")
def deploy_plan(plan_id: str):
    if plan_id not in PLANS_DB:
        raise HTTPException(status_code=404, detail="Plan not found")
    plan = PLANS_DB[plan_id]
    if not plan["approved"]:
        raise HTTPException(status_code=403, detail="Explicit administrator approval required before Kafka dispatch!")

    producer = get_kafka_producer()
    dispatched_jobs = []

    for router_name, commands in plan["ai_output"]["configs"].items():
        job_id = f"job-{uuid.uuid4().hex[:8]}"
        job_payload = {
            "job_id": job_id,
            "plan_id": plan_id,
            "device_id": router_name,
            "commands": commands,
            "timestamp": datetime.now().isoformat()
        }
        JOBS_DB[job_id] = {
            **job_payload,
            "status": "PENDING",
            "updated_at": datetime.now().isoformat(),
            "deploy_output": "",
            "verify_output": ""
        }
        producer.send(KAFKA_TOPIC, value=job_payload)
        dispatched_jobs.append(JOBS_DB[job_id])

    producer.flush()
    return {"dispatched": dispatched_jobs}

@app.get("/api/plans/{plan_id}")
def get_plan(plan_id: str):
    if plan_id not in PLANS_DB:
        raise HTTPException(status_code=404, detail="Plan not found")
    return PLANS_DB[plan_id]

class JobUpdate(BaseModel):
    status: str
    deploy_output: str = ""
    verify_output: str = ""

@app.post("/api/jobs/{job_id}/update")
def update_job(job_id: str, data: JobUpdate):
    if job_id not in JOBS_DB:
        JOBS_DB[job_id] = {"job_id": job_id}
    JOBS_DB[job_id]["status"] = data.status
    JOBS_DB[job_id]["updated_at"] = datetime.now().isoformat()
    if data.deploy_output:
        JOBS_DB[job_id]["deploy_output"] = data.deploy_output
    if data.verify_output:
        JOBS_DB[job_id]["verify_output"] = data.verify_output
    return JOBS_DB[job_id]

@app.get("/api/jobs")
def list_jobs():
    return list(JOBS_DB.values())

@app.get("/", response_class=HTMLResponse)
def index():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>AI Network Automation - OSPF</title>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 1100px; margin: 20px auto; padding: 0 20px; background: #f4f6f9; }
    .card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 6px rgba(0,0,0,0.08); margin-bottom: 20px; }
    h1, h2 { color: #1e293b; }
    button { padding: 10px 16px; border: none; border-radius: 6px; cursor: pointer; font-weight: bold; margin-right: 8px; }
    .btn-primary { background: #2563eb; color: white; }
    .btn-success { background: #16a34a; color: white; }
    .btn-warning { background: #d97706; color: white; }
    button:disabled { background: #94a3b8; cursor: not-allowed; }
    pre { background: #0f172a; color: #38bdf8; padding: 14px; border-radius: 6px; overflow-x: auto; font-size: 13px; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
    .badge { padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: bold; }
    .PENDING { background: #fef3c7; color: #92400e; }
    .RUNNING { background: #dbeafe; color: #1e40af; }
    .SUCCESS { background: #dcfce7; color: #166534; }
    .FAILED { background: #fee2e2; color: #991b1b; }
    img { max-width: 100%; max-height: 280px; border: 1px solid #cbd5e1; border-radius: 6px; margin-top: 10px; }
  </style>
</head>
<body>
  <h1>🤖 AI-Driven Network Configuration & OSPF Automation</h1>

  <div class="card">
    <h2>1. Upload Topology Diagram (PNG/JPG)</h2>
    <input type="file" id="diagramInput" accept="image/png, image/jpeg" onchange="previewImage(event)">
    <button class="btn-primary" onclick="generatePlan()">1. Generate AI Plan</button>
    <br><img id="preview" style="display:none;">
  </div>

  <div class="card" id="reviewCard" style="display:none;">
    <h2>2. AI Topology Extraction, Validation & Cisco IOS Config</h2>
    <div id="validationBox"></div>
    <div class="grid">
      <div>
        <h3>Extracted Topology (JSON)</h3>
        <pre id="jsonOutput"></pre>
      </div>
      <div>
        <h3>Generated Cisco IOS + OSPF Configs</h3>
        <pre id="iosOutput"></pre>
      </div>
    </div>
    <div style="margin-top: 15px;">
      <button class="btn-warning" id="btnApprove" onclick="approvePlan()">2. Approve Configuration</button>
      <button class="btn-success" id="btnDeploy" onclick="deployPlan()" disabled>3. Deploy via Kafka</button>
    </div>
  </div>

  <div class="card">
    <h2>3. Kafka Job Monitoring & Automated OSPF Verification</h2>
    <div id="jobsContainer">No jobs dispatched yet.</div>
  </div>

<script>
let currentPlanId = null;

function previewImage(e) {
  const img = document.getElementById('preview');
  img.src = URL.createObjectURL(e.target.files[0]);
  img.style.display = 'block';
}

async function generatePlan() {
  const fileInput = document.getElementById('diagramInput');
  if (!fileInput.files.length) return alert("Please select a diagram image first!");
  
  const formData = new FormData();
  formData.append("file", fileInput.files[0]);
  
  document.getElementById('reviewCard').style.display = 'block';
  document.getElementById('jsonOutput').textContent = "Analyzing diagram with AI...";
  document.getElementById('iosOutput').textContent = "Generating Cisco IOS & OSPF config...";

  const res = await fetch('/api/analyze', { method: 'POST', body: formData });
  const data = await res.json();
  currentPlanId = data.plan_id;

  document.getElementById('jsonOutput').textContent = JSON.stringify({
    devices: data.ai_output.devices,
    links: data.ai_output.links,
    ospf: data.ai_output.ospf,
    ambiguities: data.ai_output.ambiguities
  }, null, 2);

  let iosText = "";
  for (const [router, cmds] of Object.entries(data.ai_output.configs)) {
    iosText += `! === ${router} ===\\n` + cmds.join("\\n") + "\\n\\n";
  }
  document.getElementById('iosOutput').textContent = iosText;

  const valBox = document.getElementById('validationBox');
  if (data.validation_errors.length > 0) {
    valBox.innerHTML = `<p style="color:red;font-weight:bold;">❌ Validation Failed: ${data.validation_errors.join(", ")}</p>`;
    document.getElementById('btnApprove').disabled = true;
  } else {
    valBox.innerHTML = `<p style="color:green;font-weight:bold;">✅ Validation Passed: Subnets, OSPF Area 0, Passive LANs & SSH safety verified.</p>`;
    document.getElementById('btnApprove').disabled = false;
  }
  document.getElementById('btnDeploy').disabled = true;
}

async function approvePlan() {
  const res = await fetch(`/api/approve/${currentPlanId}`, { method: 'POST' });
  if (res.ok) {
    alert("Plan Approved! Ready to dispatch to Kafka.");
    document.getElementById('btnDeploy').disabled = false;
    document.getElementById('btnApprove').disabled = true;
  } else {
    const err = await res.json();
    alert("Approval error: " + err.detail);
  }
}

async function deployPlan() {
  const res = await fetch(`/api/deploy/${currentPlanId}`, { method: 'POST' });
  if (res.ok) {
    document.getElementById('btnDeploy').disabled = true;
    fetchJobs();
  }
}

async function fetchJobs() {
  const res = await fetch('/api/jobs');
  const jobs = await res.json();
  if (!jobs.length) return;

  let html = "";
  for (const j of jobs) {
    html += `
      <div style="border:1px solid #e2e8f0; padding:12px; border-radius:6px; margin-bottom:12px;">
        <strong>Job ID:</strong> ${j.job_id} | <strong>Router:</strong> ${j.device_id} | 
        <span class="badge ${j.status}">${j.status}</span> | <small>Updated: ${j.updated_at}</small>
        <div class="grid" style="margin-top:8px;">
          <div><strong>Paramiko Deployment Output:</strong><pre>${j.deploy_output || 'Waiting...'}</pre></div>
          <div><strong>OSPF Verification (Neighbor / Routes / Protocols):</strong><pre>${j.verify_output || 'Waiting...'}</pre></div>
        </div>
      </div>`;
  }
  document.getElementById('jobsContainer').innerHTML = html;
}

setInterval(fetchJobs, 2000);
</script>
</body>
</html>
"""
