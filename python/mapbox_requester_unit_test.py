import unittest
import requester


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

    def test_fire_single_request_to_obtain_coordinates(self):
        sample_address = "1600 Pennsylvania Ave Washington, DC"
        computed_value = requester.fire_single_request_to_obtain_coordinates(sample_address)
        expected_value = {
            "coordinates": [-76.981041, 38.878649],
            "name": "Pennsylvania Ave SE",
            "place_name": "1600 Pennsylvania Ave SE, Washington, District of Columbia 20003, United States",
            "wikidata": "",
            "type": ""
        }

        self.assertEqual(computed_value, expected_value)

if __name__ == '__main__':
    unittest.main()
