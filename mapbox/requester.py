'''
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

from mapbox import Geocoder
import uuid
import urllib.parse
import queue
import time
import pause
import sys
import weakref
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

geocoder = Geocoder(
	access_token='pk.eyJ1IjoicmFkc2NoZWl0IiwiYSI6ImNqMXRoMnZ2aDAwMW0zM3FyMm9oMzJ0MTEifQ.xdZLGZA5FNwi97t2UC4ogw')


class MapboxSubject:
	instances = []
	def __init__(
		self,
		identifier=uuid.uuid4(),
		name = "",
		place_name ="",
		request_string = "",
		coordinates = ""
	):
		self.__class__.instances.append(weakref.proxy(self))
		self.uuid = identifier
		self.name = name
		self.place_name = place_name
		self.request_string = request_string
		self.coordinates = coordinates

	def add_request_string(self, request):
		self.request_string = request

	def add_geocode(self, coordinates):
		self.coordinates = coordinates
		print(self.coordinates)


def is_not_blank(some_string):
	if some_string and some_string.strip():
		return True
	return False


def generate_request(
	name = "",
	address_line_1 = "",
	address_line_2 = "",
	postal_code = "",
	city = "",
	country = "de"
):
	request_line = (name + " ") if is_not_blank(name) else ""
	request_line += (address_line_1 + " ") if is_not_blank(address_line_1) else ""
	request_line += (address_line_2 + " ") if is_not_blank(address_line_2) else ""
	request_line += (postal_code + " ") if is_not_blank(postal_code) else ""
	request_line += (city + " ") if is_not_blank(city) else ""
	request_line += (country + " ") if is_not_blank(country) else ""

	return request_line


def process_api_response(response):
	try:
		coordinates = response.json()['features'][0]['center'] or ""
		name = response.json()['features'][0]['text'] or ""
		place_name = response.json()['features'][0]['place_name'] or ""

		if isinstance(coordinates, list):
			print(coordinates[0])
			print(coordinates[1])
			coordinates = ";".join(str(x) for x in coordinates)
			print(coordinates)
		else:
			coordinates = ""

		return {
			"coordinates": coordinates,
			"name": name,
			"place_name": place_name,
		}
	except OSError as err:
		print("OS error: {0}".format(err))
	except ValueError:
		print("Value Error")
	except IndexError:
		pass
	except:
		print("Unexpected error:", sys.exc_info()[0])


def fire_single_request_to_obtain_coordinates(request_string):
	if is_not_blank(request_string):
		urllib.parse.quote_plus(request_string.encode('utf-8'))

		response = geocoder.forward(
			request_string,
			limit = 1,
			types = ('place', 'locality', 'neighborhood', 'address', 'region', 'district')
		)
		status = evaluate_geocoding_response_status(response)

		if status == "Pause":
			fire_single_request_to_obtain_coordinates(request_string)
		elif status == "Error":
			print("Error on request " + request_string + "\n")
		else:

			response = process_api_response(response)
			print("---- API ----")
			print(response)
		return response
	else:
		return False


def evaluate_geocoding_response_status(response):
	if response.status_code == 200:
		print(response.json())
		return {
			"response": response,
		}
	elif response.status_code == 429:
		if response.headers['x-rate-reset']:
			print ("Let's wait until" + response.headers['x-rate-reset'])
			pause.until(response.headers['x-rate-reset'])
			return "Pause"
		else:
			return "Error"

	else:
		return False


def generate_subjects_from_ortschaften():
	filename = 'resources/orte.txt'

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

		return True

	return False


def execute_geocoding(wait_list_geocoding):
	if not wait_list_geocoding.empty():
		while not wait_list_geocoding.empty():
			print(wait_list_geocoding.get())
		return True
	else:
		return False


def main():

	auth_provider = PlainTextAuthProvider(username='bp2016n1', password='N6J#*b9kkU^EfVany7fVscP@mWM6&6B2taJhZ*x^$gTck*@dWR&qYW9yWz5s#2eZ') 
	cluster = Cluster(['172.16.64.61'], auth_provider=auth_provider)
	session = cluster.connect('datalake')

	wait_list_geocoding = queue.Queue()
	subjects = generate_subjects_from_ortschaften()
	fill_up_wait_list_geocoding(wait_list_geocoding, subjects)

	while not wait_list_geocoding.empty():
		subject = wait_list_geocoding.get()
		request = generate_request(subject.name)
		response = fire_single_request_to_obtain_coordinates(request)

		if response:
			try:
				print(response['coordinates'])
				subject.add_geocode(repr(response['coordinates']))
				#session.execute(
					#"""
					#INSERT INTO geocodes (id, geo_coords, name, place_name)
					#VALUES (%s, %s, %s, %s)
					#""",
					#(subject.uuid, subject.coordinates, subject.name, subject.place_name)
					#)
			except OSError as err:
				print("OS error: {0}".format(err))
			except ValueError:
				print("Value Error")
			except:
				print("Unexpected error2:", sys.exc_info()[0])
				raise

	for subject in A.instances:
		print(subject.name)

if __name__ == '__main__':
	main()
