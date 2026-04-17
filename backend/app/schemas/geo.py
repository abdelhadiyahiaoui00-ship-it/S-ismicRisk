from pydantic import BaseModel


class WilayaBasic(BaseModel):
    code: str
    name: str


class CommuneBasic(BaseModel):
    code: str
    name: str
    zone_sismique: str


class ZoneLookupResponse(BaseModel):
    wilaya_code: str
    commune: str
    zone: str
