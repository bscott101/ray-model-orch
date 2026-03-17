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
