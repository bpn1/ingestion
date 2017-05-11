from mapbox import Geocoder
import uuid
import urllib
import Queue
import time
# from cassandra.cluster import Cluster


# cluster = Cluster()
geocoder = Geocoder(
    access_token='pk.eyJ1IjoicmFkc2NoZWl0IiwiYSI6ImNqMXRoMnZ2aDAwMW0zM3FyMm9oMzJ0MTEifQ.xdZLGZA5FNwi97t2UC4ogw')


class MapboxSubject:
    def __init__(
            self,
            identifier=uuid.uuid4(),
            name="",
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


def process_api_response(response):
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


def fire_single_request_to_obtain_coordinates(request_string):
    if is_not_blank(request_string):
        urllib.quote(request_string.encode('utf-8'))

        response = geocoder.forward(
            request_string,
            limit = 1,
            types = ('place', 'locality', 'neighborhood', 'address', 'region', 'district')

        )
        status = evaluate_geocoding_response_status(response)
        return response
    else:
        return False


def evaluate_geocoding_response_status(response):
    if response.status_code == 200:
        print response.json()
        return {
            "response": response,
        }
    elif response.status_code == 429:
        return {
            "response": 429,
            "reset_time": response.headers['x-rate-limit-reset']
        }
    else:
        return False


def generate_subjects_from_ortschaften():
    filename = 'resources/ortschaften.txt'

    with open(filename) as f:
        subjects = f.readlines()
        return subjects
    return False


def fill_up_wait_list_geocoding(wait_list_geocoding, subjects):

    if subjects:
        subjects = [x.strip() for x in subjects]

        for subject in subjects:
            mapbox_subject = MapboxSubject(uuid.uuid4(), subject, subject)
            wait_list_geocoding.put(mapbox_subject)

        while not wait_list_geocoding.empty():
            i = wait_list_geocoding.get()
            print i.name

        return True

    return False


def execute_geocoding(wait_list_geocoding):
    if not wait_list_geocoding.empty():
        while not wait_list_geocoding.empty():
            print wait_list_geocoding.get()
        return True
    else:
        return False


def main():
    wait_list_geocoding = Queue.Queue()
    subjects = generate_subjects_from_ortschaften()
    fill_up_wait_list_geocoding(wait_list_geocoding, subjects)


if __name__ == '__main__':
    main()
