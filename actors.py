import ray

from scheams import scheams

@ray.remote
class HelloActor:
    def __init__(self):
        self.goodbye = ray.get_actor("goodbye-actor", "test")

    def sayHello(self, input: scheams.HelloInputModel):
        return f"Hello, {input.name}, this cluster has been expecting you"

    def sayGoodbye(self, input: scheams.HelloInputModel):
        goobyeModel = GoodbyeInputModel(name=input.name)
        res: GoodbyeOutputModel = ray.get(self.goodbye.pipeline.remote(goobyeModel))

        return f"Goodbye, {res.goodbyeResult}, ya prick."

    def pipeline(self, input: scheams.HelloInputModel) -> scheams.HelloOutputModel:
        return scheams.HelloOutputModel(
            helloResult= self.sayHello(input),
            goodbyeResult=self.sayGoodbye(input)
        )

@ray.remote
class GoodbyeActor:
    def __init__(self):
        pass

    def sayGoodbye(self, input: scheams.GoodbyeInputModel) -> str:
        return f"Goodbye, {input.name}, ya prick."


    def pipeline(self, input: scheams.GoodbyeInputModel) -> scheams.GoodbyeOutputModel:
        return GoodbyeOutputModel(goodbyeResult=self.sayGoodbye(input))


if __name__ == "__main__":
    from scheams.scheams import *

    runtime_env = {"working_dir": "."}
    ray.init("ray://localhost:10001", runtime_env=runtime_env)

    actors = ray.util.list_named_actors(True)
    
    for actor in actors:
        x = ray.get_actor(**actor)
        ray.kill(x)

    handler_good = GoodbyeActor.options(name="goodbye-actor", lifetime="detached", namespace="test").remote()
    handler_hello = HelloActor.options(name="hello-actor", lifetime="detached", namespace="test").remote()