from pydantic import BaseModel

class HelloInputModel(BaseModel):
    name: str


def main():
    import ray

    ray.init("ray://localhost:10001")
    
    print("Getting actor")
    actor = ray.get_actor(name="hello-actor", namespace="test")
    print("Got actor")

    x = HelloInputModel(name="test")
    
    print("started pipeline")
    result = ray.get(actor.pipeline.remote(x))
    print(f"Result: {result}")

if __name__ == "__main__":
    main()
