from re import X
import ray
from fastapi import FastAPI
from scheams import scheams
import time


ray.init("ray://localhost:10001", runtime_env={"working_dir": "."})
actors = ray.util.list_named_actors(True)
app = FastAPI()

@app.get("/")
def default_main():
    return {"works": "yeah"}


@app.post("/infer/")
def infer(x: scheams.HelloInputModel):
    start = time.perf_counter()
    actor = ray.get_actor(name="hello-actor", namespace="test")
    res = ray.get(actor.pipeline.remote(x))
    res = res.model_dump()
    res["end_time"] = time.perf_counter() - start

    return res

@app.post("/goodbye/")
def good(x: scheams.GoodbyeInputModel):
    start = time.perf_counter()
    actor = ray.get_actor(name="goodbye-actor", namespace="test")
    res = ray.get(actor.pipeline.remote(x))
    res = res.model_dump()
    res["end_time"] = time.perf_counter() - start

    return res