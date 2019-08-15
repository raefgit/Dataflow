import apache_beam as beam
import logging
from random import randint
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types


PROJECT = 'elite-impact-224414'
schema = 'name : STRING, id : STRING, date : STRING,title : STRING, text: STRING,magnitude : STRING, score : STRING'
src_path = "gs://amazoncustreview/DatafinitiElectronicsProductData.csv"


class Split(beam.DoFn):
    def process(self, elements):
        for element in elements:
            element = element.split(",")
            client = language.LanguageServiceClient()
            document = types.Document(content=element[21],
                                      type=enums.Document.Type.PLAIN_TEXT)
            sentiment = client.analyze_sentiment(document).document_sentiment
            return [{
                'name': element[23],
                'title': element[22],
                'id': element[0],
                'date': element[5],
                'text': element[21],
                'magnitude': str(sentiment.magnitude),
                'score': str(sentiment.score)
            }]



def main():
    BUCKET = 'amazoncustreview'
    argv = [
      '--project={0}'.format(PROJECT),
      '--staging_location=gs://{0}/staging/'.format(BUCKET),
      '--temp_location=gs://{0}/staging/'.format(BUCKET),
      '--runner=DataflowRunner',
      '--job_name=sentimentanalysis2',
      '--save_main_session',
      '--requirements_file=requirements.txt',
      '--num_workers=1'
    ]
    p = beam.Pipeline(argv=argv)

    (p
       | 'ReadData' >> beam.io.textio.ReadFromText(src_path)
       | 'AddKeys' >> beam.Map(lambda x: (randint(0, 700), x))
       | 'MakeBundles' >> beam.GroupByKey()
       | 'DropKeys' >> beam.Map(lambda (k, bundle): bundle)
       | 'ParseCSV' >> beam.ParDo(Split())
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery('{0}:customer_review.customerReviewResult'.format(PROJECT),
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )
    p.run()


if __name__ == '__main__':
    logger = logging.getLogger().setLevel(logging.INFO)
    main()
