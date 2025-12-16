from pydantic import BaseModel


class CreateSessionResponse(BaseModel):
    session_id: str

class ExecRequest(BaseModel):
    session_id: str
    code: str
    timeout_sec: float = 10.0  # на один вызов