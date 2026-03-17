import ray
from pydantic import BaseModel

class HelloInputModel(BaseModel):
    name: str

class HelloOutputModel(BaseModel):
    helloResult: str
    goodbyeResult: str


class GoodbyeInputModel(BaseModel):
    name: str

class GoodbyeOutputModel(BaseModel):
    goodbyeResult: str


@ray.remote
class HelloActor:
    def __init__(self):
        self.goodbye = ray.get_actor("goodbye-actor", "test")

    def sayHello(self, input: HelloInputModel):
        return f"Hello, {input.name}, this cluster has been expecting you"

    def sayGoodbye(self, input: HelloInputModel):
        goobyeModel = GoodbyeInputModel(name=input.name)
        res: GoodbyeOutputModel = ray.get(self.goodbye.pipeline.remote(goobyeModel))

        return f"Goodbye, {res.goodbyeResult}, ya prick."

    def pipeline(self, input: HelloInputModel) -> HelloOutputModel:
        return HelloOutputModel(
            helloResult= self.sayHello(input),
            goodbyeResult=self.sayGoodbye(input)
        )

@ray.remote
class GoodbyeActor:
    def __init__(self):
        pass

    def sayGoodbye(self, input: GoodbyeInputModel) -> str:
        return f"Goodbye, {input.name}, ya prick."


    def pipeline(self, input: GoodbyeInputModel) -> GoodbyeOutputModel:
        return GoodbyeOutputModel(goodbyeResult=self.sayGoodbye(input))


if __name__ == "__main__":
    ray.init("ray://localhost:10001")

    actors = ray.util.list_named_actors(True)
    
    for actor in actors:
        x = ray.get_actor(**actor)
        ray.kill(x)

    handler_good = GoodbyeActor.options(name="goodbye-actor", lifetime="detached", namespace="test").remote()
    handler_hello = HelloActor.options(name="hello-actor", lifetime="detached", namespace="test").remote()