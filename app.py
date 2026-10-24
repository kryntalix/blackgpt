import json
import re
 
import requests
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, StreamingResponse
from g4f import Provider
from g4f.client import Client
 
app = FastAPI()
client = Client()
 
MODELS = ["gpt-4o-mini", "gpt-4o", "deepseek-chat", "llama-3.3-70b"]
POLLINATIONS_URL = "https://text.pollinations.ai/openai"
 
# Ba'zi servislar xato o'rniga oddiy matn qaytaradi ("Sign up and repeat...").
JUNK = re.compile(
    r"sign ?up|log ?in|repeat (you|your) request|rate limit|too many requests|"
    r"captcha|cloudflare|access denied|api key|subscribe|unauthorized|try again later",
    re.I,
)
 
 
def pollinations_stream(msgs):
    """Pollinations: ro'yxatdan o'tish va kalit talab qilinmaydi."""
    r = requests.post(
        POLLINATIONS_URL,
        json={"model": "openai", "messages": msgs, "stream": True},
        stream=True,
        timeout=60,
    )
    r.raise_for_status()
    for line in r.iter_lines(decode_unicode=True):
        if not line or not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if data == "[DONE]":
            break
        try:
            choices = json.loads(data).get("choices") or []
        except ValueError:
            continue
        if choices:
            t = (choices[0].get("delta") or {}).get("content")
            if t:
                yield t
 
 
def g4f_stream(model, provider, msgs):
    kw = {"provider": provider} if provider else {}
    stream = client.chat.completions.create(
        model=model, messages=msgs, stream=True, **kw
    )
    for ch in stream:
        t = ch.choices[0].delta.content if ch.choices else None
        if t:
            yield t
 
 
def sources(model, msgs):
    yield lambda: pollinations_stream(msgs)
    for m in [model] + [x for x in MODELS if x != model]:
        yield lambda m=m: g4f_stream(m, None, msgs)
    for name, m in [("PollinationsAI", "openai"), ("Yqcloud", "gpt-4")]:
        p = getattr(Provider, name, None)
        if p:
            yield lambda m=m, p=p: g4f_stream(m, p, msgs)
 
 
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
 
    def gen():
        last = None
        for src in sources(model, msgs):
            buf, started = "", False
            try:
                for t in src():
                    if started:
                        yield t
                        continue
                    buf += t
                    if len(buf) >= 120:
                        started = True
                        yield buf
                if started:
                    return
                if buf and not JUNK.search(buf):
                    yield buf
                    return
                last = buf or last
            except Exception as e:
                last = e
                if started:
                    yield f"\n\n[Xato: {e}]"
                    return
        yield f"Hech bir variant javob bermadi. Oxirgi javob/xato: {last or ''}"
 
    return StreamingResponse(gen(), media_type="text/plain; charset=utf-8")
 
