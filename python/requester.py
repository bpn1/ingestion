from mapbox import Geocoder
import uuid
import urllib

geocoder = Geocoder(
    access_token='pk.eyJ1IjoicmFkc2NoZWl0IiwiYSI6ImNqMXRoMnZ2aDAwMW0zM3FyMm9oMzJ0MTEifQ.xdZLGZA5FNwi97t2UC4ogw')


class MapboxSubject:
    def __init__(
            self,
            name="",
            identifier=uuid.uuid4(),
            request_string="",
            latitude="",
            longitude=""
    ):
        self.uuid = identifier
        self.name = name
        self.request_string = request_string
        self.coordinates = [latitude, longitude]

    def add_request_string(self, request):
        self.request_string = request

    def add_geocode(self, latitude, longitude):
        self.coordinates = [latitude, longitude]


def is_not_blank(some_string):
    if some_string and some_string.strip():
        return True
    return False


def generate_request(
        address_line_1="",
        address_line_2="",
        postal_code="",
        city="",
        country="de"
):
    requestLine = ""

    if is_not_blank(address_line_1):
        requestLine += ""


def fire_single_request_to_obtain_coordinates(request_string):
    if is_not_blank(request_string):
        urllib.quote(request_string.encode('utf-8'))
        response = geocoder.forward(
            request_string,
            limit = 1,
            types = ('place', 'locality', 'neighborhood', 'address', 'region', 'district')

        )

        if response.status_code == 200:
            if 'center' in response.json()['features'][0]:
                coordinates = response.json()['features'][0]['center']
            else:
                coordinates = ""

            if 'text' in response.json()['features'][0]:
                name = response.json()['features'][0]['text']
            else:
                name = ""

            if 'place_name' in response.json()['features'][0]:
                place_name = response.json()['features'][0]['place_name']
            else:
                place_name = ""

            if 'properties' in response.json()['features'][0]:
                properties = response.json()['features'][0]['properties']

                if 'wikidata' in properties:
                    wikidata = properties[0]['wikidata']
                else:
                    wikidata = ""


                # Filter for the right type, according https://www.mapbox.com/api-documentation/?language=Python#geocoding
                if 'type' in properties:
                    kind = properties[0]['type']
                else:
                    kind = ""
            else:
                wikidata = ""
                kind = ""

            return {
                "coordinates": coordinates,
                "name": name,
                "place_name": place_name,
                "wikidata": wikidata,
                "type": kind
            }
        else:
            return False
    else:
        return False


def main():
    test = ""


if __name__ == '__main__':
    main()
