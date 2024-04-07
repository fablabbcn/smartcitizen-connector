from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel

class Measurement(BaseModel):
    id: int
    name: str
    description: str

class Metric(BaseModel):
    id: Optional[int] = None
    name: str
    description: Optional[str] = ''
    module: Optional[str] = "scdata.device.process"
    function: str
    unit: Optional[str] = ''
    post: Optional[bool] = False
    args: Optional[dict] = None
    kwargs: Optional[dict] = None

class Sensor(BaseModel):
    id: int
    name: str
    description: str
    unit: Optional[str] = None
    measurement_id: Optional[int] = None
    measurement: Optional[Measurement] = None
    value: Optional[float] = None
    prev_value: Optional[float] = None
    last_reading_at: Optional[datetime] = None
    tags: Optional[List[str]] = []
    default_key: Optional[str] = []

class Kit(BaseModel):
    id: int
    slug: str
    name: str
    description: str
    created_at: datetime
    updated_at: datetime
    sensors: Optional[List[Sensor]] = None

class Owner(BaseModel):
    id: int
    username: str
    role: Optional[str] = ""
    devices: Optional[List[str]] = None

class Location(BaseModel):
    city: str
    country_code: str
    country: str
    exposure: Optional[str] = None
    elevation: Optional[float] = None
    geohash: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

class HardwareInfo(BaseModel):
    id: str
    mac: str
    time: str
    esp_bd: str
    hw_ver: str
    sam_bd: str
    esp_ver: str
    sam_ver: str
    rcause: Optional[str] = None

class HardwareVersion(BaseModel):
    from_date: Optional[datetime] = None
    to_date: Optional[datetime] = None
    ids: Optional[dict] = None

class HardwarePostprocessing(BaseModel):
    blueprint_url: Optional[str] = None
    description: Optional[str] = None
    versions: Optional[List[HardwareVersion]] = None
    forwarding: Optional[str] = None

class Postprocessing(BaseModel):
    id: int
    blueprint_url: Optional[str] = None
    hardware_url: Optional[str] = None
    forwarding_params: Optional[int] = None
    meta: Optional[str] = None
    latest_postprocessing: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime

class Data(BaseModel):
    sensors: Optional[List[Sensor]] = None

class Notifications(BaseModel):
    low_battery: bool
    stopped_publishing: bool

class Device(BaseModel):
    id: int
    uuid: str
    name: str
    description: str
    state: str
    postprocessing: Optional[Postprocessing] = None
    hardware_info: Optional[HardwareInfo] = None
    system_tags: List[str]
    user_tags: List[str]
    is_private: bool
    notify: Notifications
    last_reading_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: datetime
    owner: Owner
    data: Data
    location: Optional[Location]= None
    kit: Optional[Kit] = None
    device_token: Optional[str] = ''
