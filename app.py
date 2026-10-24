from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, StreamingResponse
from g4f.client import Client

app = FastAPI()
client = Client()

MODELS = ["gpt-4o-mini", "gpt-4o", "deepseek-chat", "llama-3.3-70b"]


@app.get("/")
def index():
    return FileResponse("index.html")


@app.get("/models")
def models():
    return MODELS


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/chat")
async def chat(req: Request):
    body = await req.json()
    model = body.get("model") or MODELS[0]
    system = (body.get("system") or "").strip()
    msgs = body.get("messages", [])[-20:]
    if system:
        msgs = [{"role": "system", "content": system}] + msgs
    order = [model] + [m for m in MODELS if m != model]

    def gen():
        last = None
        for m in order:
            started = False
            try:
                stream = client.chat.completions.create(
                    model=m, messages=msgs, stream=True
                )
                for ch in stream:
                    t = ch.choices[0].delta.content if ch.choices else None
                    if t:
                        started = True
                        yield t
                if started:
                    return
            except Exception as e:
                last = e
                if started:
                    yield f"\n\n[Xato: {e}]"
                    return
        yield f"Hech bir model javob bermadi. {last or ''}"

    return StreamingResponse(gen(), media_type="text/plain; charset=utf-8")
