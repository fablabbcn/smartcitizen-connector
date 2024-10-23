from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel

class Measurement(BaseModel):
    id: int
    uuid: str
    name: str
    description: str
    definition: Optional[str] = None

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
    uuid: str
    name: str
    description: str
    unit: Optional[str] = None
    measurement: Optional[Measurement] = None
    datasheet: Optional[str] = None
    unit_definition: Optional[str] = None
    value: Optional[float] = None
    prev_value: Optional[float] = None
    last_reading_at: Optional[datetime] = None
    tags: Optional[List[str]] = []
    default_key: Optional[str] = []

class Owner(BaseModel):
    id: int
    username: str
    role: Optional[str] = ""
    devices: Optional[List[str]] = None

class Location(BaseModel):
    city: Optional[str] = None
    country_code: Optional[str] = None
    country: Optional[str] = None
    exposure: Optional[str] = None
    elevation: Optional[float] = None
    geohash: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

class HardwareStatus(BaseModel):
    id: str
    mac: str
    time: str
    esp_bd: str
    hw_ver: str
    sam_bd: str
    esp_ver: str
    sam_ver: str
    rcause: Optional[str] = None

class HardwareInfo(BaseModel):
    name: str
    type: str
    version: Optional[str] = None
    slug: str
    # last_status_message: Optional[HardwareStatus] = 'FILTERED'

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
    id: Optional[int] = None
    blueprint_url: Optional[str] = None
    hardware_url: Optional[str] = None
    forwarding_params: Optional[int] = None
    meta: Optional[str] = None
    latest_postprocessing: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

class Data(BaseModel):
    sensors: Optional[List[Sensor]] = None

class Notifications(BaseModel):
    low_battery: bool
    stopped_publishing: bool

class Policy(BaseModel):
    is_private: Any
    precise_location: Any
    enable_forwarding: Any

class Device(BaseModel):
    id: int
    uuid: str
    name: str
    description: str
    state: str
    postprocessing: Optional[Postprocessing] = None
    hardware: HardwareInfo
    system_tags: List[str]
    user_tags: List[str]
    data_policy: Optional[Policy] = None
    notify: Notifications
    last_reading_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: datetime
    owner: Optional[Owner] = None
    data: Optional[Data] = None
    location: Optional[Location]= None
    device_token: str = 'FILTERED'

class Experiment(BaseModel):
    id: Optional[int] = None
    name: str
    description: Optional[str] = ''
    owner_id: Optional[int] = None
    active: Optional[bool] = None
    is_test: bool
    starts_at: Optional[datetime] = None
    ends_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    device_ids: Optional[List[int]] = None

class User(BaseModel):
    id: int
    uuid: str
    username: str
    role: Optional[str] = ""
    devices: Optional[List[Device]] = None
    profile_picture: str
    location: Location
    updated_at: datetime
    forwarding_token: str
    forwarding_username: str
