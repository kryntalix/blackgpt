import re
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, StreamingResponse
from g4f.client import Client
from g4f import Provider
 
app = FastAPI()
client = Client()
 
MODELS = ["gpt-4o-mini", "gpt-4o", "deepseek-chat", "llama-3.3-70b"]
 
# Ba'zi providerlar xato o'rniga oddiy matn qaytaradi ("Sign up and repeat...").
# Shunday javoblar aniqlansa, keyingi variantga o'tiladi.
JUNK = re.compile(
    r"sign ?up|log ?in|repeat (you|your) request|rate limit|too many requests|"
    r"captcha|cloudflare|access denied|api key|subscribe|unauthorized|try again later",
    re.I,
)
 
 
def candidates(model):
    out = [(model, None)] + [(m, None) for m in MODELS if m != model]
    for name, m in [
        ("PollinationsAI", "openai"),
        ("PollinationsAI", "gpt-4o-mini"),
        ("Yqcloud", "gpt-4"),
    ]:
        p = getattr(Provider, name, None)
        if p:
            out.append((m, p))
    return out
 
 
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
        for m, p in candidates(model):
            buf, started = "", False
            try:
                kw = {"provider": p} if p else {}
                stream = client.chat.completions.create(
                    model=m, messages=msgs, stream=True, **kw
                )
                for ch in stream:
                    t = ch.choices[0].delta.content if ch.choices else None
                    if not t:
                        continue
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
