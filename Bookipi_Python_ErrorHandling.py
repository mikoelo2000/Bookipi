import logging

class ExtractMongoData(beam.DoFn):
    def process(self, element):
        try:
            # MongoDB connection and extraction logic
        except Exception as e:
            logging.error(f"Error extracting data: {e}")
            # Handle error or raise it

class TransformData(beam.DoFn):
    def process(self, element):
        try:
            # Transformation logic
        except Exception as e:
            logging.error(f"Error transforming data: {e}")
            # Handle error or raise it
