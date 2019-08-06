from __future__ import absolute_import
# import argparse
import logging
import math
from apache_beam.io import fileio
import apache_beam as beam
from apache_beam import DoFn
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
from apache_beam.options.pipeline_options import PipelineOptions


class ImageExtract(DoFn):

    def process(self, element, tracker=DoFn.RestrictionParam):
        from apache_beam.io.gcp import gcsio
        import zipfile
        logging.info(element)
        # logging.info(len(element))

        with gcsio.GcsIO().open(element.path, 'r') as f:
            if zipfile.is_zipfile(f):
                logging.info('it is a zip file')
                z = zipfile.ZipFile(f)
                file_list = z.filelist
                for some_file in file_list:
                    print some_file.filename
                if '.TIF' in some_file.filename:
                    logging.info(some_file.filename)
                    try:
                        data = z.read(some_file)
                    except KeyError:
                        logging.info('ERROR: Did not find %s in zip file' % some_file.filename)
                    else:
                        logging.info('zip file is : %s', some_file.filename)
                        outfile = 'gs://dataflow-buffer/python-6/' + some_file.filename
                        with gcsio.GcsIO().open(outfile, mode='w',
                                                mime_type='image/tiff') as writing_path:
                            writing_path.write(data)
                            logging.info( 'write success : %s', some_file.filename)


def gen():
    for i in range(50):
        yield beam.Create([i])


def run():
    p = beam.Pipeline(options=PipelineOptions())
    gcs = GCSFileSystem(PipelineOptions())
    pattern_1 = [
        'gs://dataflow-buffer/parent-unpack/2018/i20180130/PxpFJwJabD-untarI20180130/DESIGN/USD0808610-20180130.ZIP']
    input_pattern = ['gs://dataflow-buffer/parent-unpack/2018/i20180130/PxpFJwJabD-untar*/**/*.ZIP']
    input_pattern_1 = 'gs://dataflow-buffer/parent-unpack/2018/i20180130/PxpFJwJabD-untar*/**/*.ZIP'

    parent_zip = 'gs://bulk_pdfimages_dump/bulkdata.uspto.gov/data/patent/grant/redbook/2010/I20100202.zip'

    result = [m.metadata_list for m in gcs.match(input_pattern)]

    metadata_list = result.pop()

    print 'satya'
    parts = (p
             # | 'Match Files' >> fileio.MatchFiles(pattern_1)
             | 'Return nested files' >> beam.Create(metadata_list)
             # | 'print Files' >> beam
             | 'Print read file' >> beam.ParDo(ImageExtract())
             # | 'one' >> beam.Map()
             )

    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
