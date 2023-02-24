import pandas as pd

class DataConverter:
    
    @staticmethod
    def convert_json_to_csv(json_data):
        """Convert the JSON data to CSV
        """
        # Using Pandas to Convert a JSON String to a CSV File
        df = pd.read_json(json_data)
        csv_data = df.to_csv(index=False)

        return csv_data
