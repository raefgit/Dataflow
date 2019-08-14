import apache_beam as beam
import logging
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types


PROJECT = 'elite-impact-224414'
schema = 'name : STRING, id : STRING, date : STRING,title : STRING, text: STRING,magnitude : STRING, score : STRING'
src_path = "gs://amazoncustreview/sentimentSource.csv"


class Split(beam.DoFn):
    def process(self, element):
        element = element.split(",")
        client = language.LanguageServiceClient()
        document = types.Document(content=element[2],
                                  type=enums.Document.Type.PLAIN_TEXT)
        sentiment = client.analyze_sentiment(document).document_sentiment
        return [{
            'name': element[0],
            'title': element[1],
            'magnitude': sentiment.magnitude,
            'score': sentiment.score
        }]


def main():
    BUCKET = 'amazoncustreview'
    argv = [
      '--project={0}'.format(PROJECT),
      '--staging_location=gs://{0}/staging/'.format(BUCKET),
      '--temp_location=gs://{0}/staging/'.format(BUCKET),
      '--runner=DataflowRunner',
      '--job_name=examplejob2',
      '--save_main_session'
    ]
    p = beam.Pipeline(argv=argv)

    (p
       | 'ReadData' >> beam.io.textio.ReadFromText(src_path)
       | 'ParseCSV' >> beam.ParDo(Split())
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery('{0}:customer_review.customerReview2'.format(PROJECT),
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )
    p.run()


if __name__ == '__main__':
    logger = logging.getLogger().setLevel(logging.INFO)
    main()
