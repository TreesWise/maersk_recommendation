from typing import Union, List
from pydantic import BaseModel
from fastapi import Query

class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Union[str, None] = None


class User(BaseModel):
    username: str


class UserInDB(User):
    hashed_password: str

class UserInput(BaseModel):
    # item_name : List=None
    # maker : List=None
    # model : List=None
    # part_num : List=None
    itemDetail : List=None
    delivery_port_list : List= None
    n_vendor : int=None

class SpareRecommendationInput(BaseModel):
    vessel_object_id : str = None
    job_plan : str = None
    equipment : str = None


class DummyInput(BaseModel):
    dummy : str = None

class YardRecommendationInput(BaseModel):
    Vessel_Type : str = None
    Sub_Type : str = None
    Length : float = None
    Breadth : float = None
    Depth : float = None
