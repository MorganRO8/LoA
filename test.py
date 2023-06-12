import json
import unittest
from unittest.mock import patch, MagicMock

# Import the main function from your program
from main import main

# Load the JSON file
with open('automatic.json') as f:
    automatic = json.load(f)

# Create a class for the tests
class TestMain(unittest.TestCase):

    # Test the main function
    def test_main(self):
        print("Testing main function...")
        # Mock the argparse.ArgumentParser.parse_args function to return the automatic dictionary
        with patch('argparse.ArgumentParser.parse_args', return_value=automatic):
            # Call the main function
            main()
        print("Main function test completed.")

    # Test the Scrape function
    def test_scrape(self):
        print("Testing Scrape function...")
        # Import the Scrape function
        from Scrape import Scrape

        # Mock the Scrape function to return a single paper
        with patch('Scrape.Scrape', return_value={'paper1': 'content1'}):
            # Call the Scrape function with the automatic dictionary
            result = Scrape(automatic['Scrape'])

            # Check that the Scrape function was called with the correct parameters
            Scrape.assert_called_with(automatic['Scrape'])

            # Check that the Scrape function returned the correct result
            self.assertEqual(result, {'paper1': 'content1'})
        print("Scrape function test completed.")

    # Test the snorkel_train function
    def test_snorkel_train(self):
        print("Testing snorkel_train function...")
        # Import the snorkel_train function
        from snorkel_train import snorkel_train

        # Mock the snorkel_train function to return a single paper
        with patch('snorkel_train.snorkel_train', return_value={'paper1': 'content1'}):
            # Call the snorkel_train function with the automatic dictionary
            result = snorkel_train(automatic['snorkel_train'])

            # Check that the snorkel_train function was called with the correct parameters
            snorkel_train.assert_called_with(automatic['snorkel_train'])

            # Check that the snorkel_train function returned the correct result
            self.assertEqual(result, {'paper1': 'content1'})
        print("snorkel_train function test completed.")

    # Test the Inference function
    def test_inference(self):
        print("Testing Inference function...")
        # Import the Inference function
        from Inference import Inference

        # Mock the Inference function to return a single paper
        with patch('Inference.Inference', return_value={'paper1': 'content1'}):
            # Call the Inference function with the automatic dictionary
            result = Inference(automatic['Inference'])

            # Check that the Inference function was called with the correct parameters
            Inference.assert_called_with(automatic['Inference'])

            # Check that the Inference function returned the correct result
            self.assertEqual(result, {'paper1': 'content1'})
        print("Inference function test completed.")

# Run the tests
if __name__ == '__main__':
    unittest.main()

# Create a test suite
suite = unittest.TestSuite()

# Add tests to the test suite
suite.addTest(TestMain('test_main'))
suite.addTest(TestMain('test_scrape'))
suite.addTest(TestMain('test_snorkel_train'))
suite.addTest(TestMain('test_inference'))

# Run the test suite
runner = unittest.TextTestRunner()
print("Running tests...")
runner.run(suite)
