import os.path
import tempfile
import unittest
from rht_data_engineer.run_pipeline import read_files_from_dir, parse_xml, Tag


class MyTest(unittest.TestCase):
    def get_data(self):
        return """
        <event>
            <order_id>104</order_id>
            <date_time>2023-08-11T12:00:00</date_time>
            <status>Completed</status>
            <cost>110.00</cost>
            <repair_details>
                <technician>Robert White</technician>
                <repair_parts>
                    <part name="Tire" quantity="2"/>
                    <part name="Brake Fluid" quantity="1"/>
                </repair_parts>
            </repair_details>
        </event>
        """

    def test_read(self):
        file_name_list = "file1.xml", "file2.xml"
        expected_count = 2
        actual_count = 0
        with tempfile.TemporaryDirectory() as tmpdirname:
            for file_name in file_name_list:
                with open(os.path.join(tmpdirname, file_name), "w") as writer:
                    print(self.get_data(), file=writer)
            for data in read_files_from_dir(tmpdirname):
                if "Robert White" in data:
                    actual_count += 1
        self.assertEqual(expected_count, actual_count, "The expected text was not found.")

    def test_parse(self):
        result_dict = parse_xml(self.get_data())
        technician = result_dict[Tag.EVENT][Tag.REPAIR_DETAILS][Tag.TECHNICIAN]
        self.assertEqual("Robert White", technician, "The name is wrong.")


if __name__ == '__main__':
    unittest.main()