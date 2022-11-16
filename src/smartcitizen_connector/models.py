from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel

class Sensor(BaseModel):
    id: int
    name: Optional[str] = None
    description: Optional[str] = None
    unit: Optional[str] = None

class Kit(BaseModel):
    id: int
    slug: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    sensors: Optional[list[Sensor]] = None

class Owner(BaseModel):
    id: int
    username: Optional[str] = None
    username: Optional[str] = None

class Location(BaseModel):
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    city: Optional[str] = None
    country_code: Optional[str] = None

class Data(BaseModel):
    location: Optional[Location]= None
    sensors: Optional[list[Sensor]] = None

class DeviceSummary(BaseModel):
    id: int
    name: Optional[str] = None
    description: Optional[str] = None
    added_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    last_reading_at: Optional[datetime] = None
    owner_id: Optional[int] = None
    owner_username: Optional[str] = None
    user_tags: Optional[List] = None
    system_tags: Optional[List] = None
    state: Optional[str] = None
    kit_id: Optional[float] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    city: Optional[str] = None
    country_code: Optional[str] = None

class Device(BaseModel):
    id: int
    name: Optional[str] = None
    description: Optional[str] = None
    added_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    last_reading_at: Optional[datetime] = None
    data: Optional[Data] = None
    owner: Optional[Owner] = None
    owner_id: Optional[int] = None
    owner_username: Optional[str] = None
    user_tags: Optional[List] = None
    system_tags: Optional[List] = None
    state: Optional[str] = None
    kit: Optional[Kit] = None
