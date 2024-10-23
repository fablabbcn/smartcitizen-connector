from requests import get, patch, post, delete
from os import environ
from smartcitizen_connector.tools import logger
from typing import Optional
import json

class HttpHandler:
    url: Optional[str] = None

    def __init__(self, path: str):
        self.path = path
        self.__set_headers__()
        if self.id is not None:
            self.url = f'{self.path}{self.id}'

    def __set_headers__(self):

        self.headers = {
            'Content-type': 'application/json'
            }

        if 'SC_BEARER' not in environ:
            logger.warning('No Auth Bearer set. Will not be able to POST, PATCH, DELETE. Include it environment variable with SC_BEARER')
            return False

        logger.info('Using Auth Bearer')
        self.headers['Authorization'] = 'Bearer ' + environ['SC_BEARER']

        return True

    def get(self):
        r = get(self.url)
        r.raise_for_status()
        return r

    def patch(self, property: str):
        r = patch(self.url,
            data=self.model.json(include=property,
                exclude_none=True),
            headers = self.headers
        )
        r.raise_for_status()
        return r

    def post(self):
        r = post(self.path,
            data=self.model.json(exclude_none=True),
            headers = self.headers)

        r.raise_for_status()
        return r

    def delete(self):
        r = delete(self.url,
            headers = self.headers)

        r.raise_for_status()
        return r
