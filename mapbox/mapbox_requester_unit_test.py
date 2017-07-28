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

import unittest
import requester
import Queue
from requests.models import Response


class MyTestCase(unittest.TestCase):
    def test_is_not_blank(self):
        computed_value_1 = requester.is_not_blank("")
        computed_value_2 = requester.is_not_blank(" ")
        computed_value_3 = requester.is_not_blank("Something")
        computed_value_4 = requester.is_not_blank(None)

        expected_value_1 = False
        expected_value_2 = False
        expected_value_3 = True
        expected_value_4 = False

        self.assertEqual(computed_value_1, expected_value_1)
        self.assertEqual(computed_value_2, expected_value_2)
        self.assertEqual(computed_value_3, expected_value_3)
        self.assertEqual(computed_value_4, expected_value_4)

    def evaluate_geocoding_response(self):

        sample_response_1 = Response()
        sample_response_1.status_code = 200

        sample_response_2 = Response()
        sample_response_2.status_code = 429
        sample_response_2.headers['x-rate-limit-reset'] = 1

        sample_response_3 = Response()
        sample_response_3.status_code = 500

        sample_response_4 = ""

        computed_value_1 = requester.evaluate_geocoding_response_status(sample_response_1)
        computed_value_2 = requester.evaluate_geocoding_response_status(sample_response_2)
        computed_value_3 = requester.evaluate_geocoding_response_status(sample_response_3)
        computed_value_4 = requester.evaluate_geocoding_response_status(sample_response_4)

        expected_value_1 = {
            "response": sample_response_1
        }

        expected_value_2 = {
            "response": sample_response_1,
            "reset_time": 1
        }

        expected_value_3 = False
        expected_value_4 = False

        self.assertEqual(computed_value_1, expected_value_1)
        self.assertEqual(computed_value_2, expected_value_2)
        self.assertEqual(computed_value_3, expected_value_3)
        self.assertEqual(computed_value_4, expected_value_4)

    def test_fire_single_request_to_obtain_coordinates(self):
        sample_request = "1600 Pennsylvania"

        computed_value = requester.fire_single_request_to_obtain_coordinates(sample_request).json()

        expected_value = {u'attribution': u'NOTICE: \xa9 2017 Mapbox and its suppliers. All rights reserved. Use of this data is subject to the Mapbox Terms of Service (https://www.mapbox.com/about/maps/). This response and the information it contains may not be retained.', u'query': [u'1600', u'pennsylvania'], u'type': u'FeatureCollection', u'features': [{u'center': [-76.981041, 38.878649], u'geometry': {u'type': u'Point', u'coordinates': [-76.981041, 38.878649], u'interpolated': True}, u'text': u'Pennsylvania Ave SE', u'place_type': [u'address'], u'properties': {}, u'context': [{u'text': u'Barney Circle', u'id': u'neighborhood.295468'}, {u'text': u'Washington', u'wikidata': u'Q61', u'id': u'place.11387590027246050'}, {u'text': u'20003', u'id': u'postcode.7407455452898840'}, {u'short_code': u'US-DC', u'text': u'District of Columbia', u'wikidata': u'Q61', u'id': u'region.3403'}, {u'short_code': u'us', u'text': u'United States', u'wikidata': u'Q30', u'id': u'country.3145'}], u'address': u'1600', u'relevance': 0.99, u'type': u'Feature', u'id': u'address.11159252373890170', u'place_name': u'1600 Pennsylvania Ave SE, Washington, District of Columbia 20003, United States'}]}


        # expected_value_1 = {
        #     "coordinates": [-76.981041, 38.878649],
        #     "name": "Pennsylvania Ave SE",
        #     "place_name": "1600 Pennsylvania Ave SE, Washington, District of Columbia 20003, United States",
        #     "wikidata": "",
        #     "type": ""
        # }

        self.assertEqual(computed_value, expected_value)

    def test_execute_geocoding(self):
        empty_test_queue = Queue.Queue()
        filled_test_queue = Queue.Queue()
        filled_test_queue.put(5)

        computed_value_1 = requester.execute_geocoding(empty_test_queue)
        computed_value_2 = requester.execute_geocoding(filled_test_queue)

        expected_value_1 = False
        expected_value_2 = True

        self.assertEqual(computed_value_1, expected_value_1)
        self.assertEqual(computed_value_2, expected_value_2)

    # def test_fill_up_wait_list_geocoding(self):
    #     expected_value = Queue.Queue()
    #     sample_subject_1 = requester.MapboxSubject("Veit im Winkel")
    #     sample_subject_2 = requester.MapboxSubject("Kastrup-Rauxel")
    #     sample_subject_3 = requester.MapboxSubject("Buxtehude")
    #     expected_value.put(sample_subject_1)
    #     expected_value.put(sample_subject_2)
    #     expected_value.put(sample_subject_3)
    #
    #     computed_value =
    #
    #     while not expected_value.empty():
    #         print expected_value.get()
    #
    #     self.assertEqual(expected_value, computed_value)

if __name__ == '__main__':
    unittest.main()
