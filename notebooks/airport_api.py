import os
import http.client

class AirportApi:

    def __init__(self, key, host):
        self.connection = http.client.HTTPSConnection("airport-info.p.rapidapi.com")

        self.headers = {
            'X-RapidAPI-Key': key,
            'X-RapidAPI-Host': host
        }

    def get_airport_info_by_iata(self, iata):
        if iata == None:
            return {}

        self.connection.request('GET', f'/airport?iata={iata}', headers = self.headers)

        response = self.connection.getresponse()

        return response.read().decode("utf-8")

    def get_airport_info_by_icao(self, icao):
        if icao == None:
            return {}

        self.connection.request('GET', f'/airport?icao={icao}', headers = self.headers)

        response = self.connection.getresponse()

        return response.read().decode("utf-8")
