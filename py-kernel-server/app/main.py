from fastapi import FastAPI, HTTPException
from app.schema import CreateSessionResponse, ExecRequest
from jupyter_client import KernelManager
import uuid
import queue
import time

app = FastAPI()

# session_id -> {"km": KernelManager, "kc": client, "created_at": ts}
SESSIONS = {}

@app.post("/sessions", response_model=CreateSessionResponse)
def create_session():
    session_id = str(uuid.uuid4())

    km = KernelManager(kernel_name="python3")
    km.start_kernel()

    kc = km.client()
    kc.start_channels()
    # простая проверка, что kernel живой
    kc.kernel_info()

    SESSIONS[session_id] = {"km": km, "kc": kc, "created_at": time.time()}
    return {"session_id": session_id}

@app.delete("/sessions/{session_id}")
def delete_session(session_id: str):
    s = SESSIONS.pop(session_id, None)
    if not s:
        return {"ok": True}

    kc = s["kc"]
    km = s["km"]

    try:
        kc.stop_channels()
    finally:
        km.shutdown_kernel(now=True)

    return {"ok": True}

@app.post("/exec")
def exec_code(req: ExecRequest):
    s = SESSIONS.get(req.session_id)
    if not s:
        raise HTTPException(status_code=404, detail="Unknown session_id")

    kc = s["kc"]
    msg_id = kc.execute(req.code)

    outputs = []
    deadline = time.time() + req.timeout_sec

    # Собираем сообщения iopub, пока не придёт idle для нашего msg_id
    while time.time() < deadline:
        try:
            msg = kc.get_iopub_msg(timeout=0.2)
        except queue.Empty:
            continue

        if msg.get("parent_header", {}).get("msg_id") != msg_id:
            continue

        msg_type = msg["header"]["msg_type"]
        content = msg["content"]

        if msg_type == "stream":
            outputs.append({"type": "stream", "name": content.get("name"), "text": content.get("text")})

        elif msg_type in ("execute_result", "display_data"):
            data = content.get("data", {})
            outputs.append({
                "type": msg_type,
                "data": {
                    "text/plain": data.get("text/plain"),
                    "image/png": data.get("image/png"),
                    "text/html": data.get("text/html"),
                }
            })

        elif msg_type == "error":
            outputs.append({
                "type": "error",
                "ename": content.get("ename"),
                "evalue": content.get("evalue"),
                "traceback": content.get("traceback"),
            })

        elif msg_type == "status" and content.get("execution_state") == "idle":
            return {"ok": True, "outputs": outputs}

    return {"ok": False, "outputs": outputs, "error": "timeout"}