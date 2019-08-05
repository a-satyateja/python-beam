from __future__ import absolute_import
# import argparse
import logging
import math
from apache_beam.io import fileio
import apache_beam as beam
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
from apache_beam.options.pipeline_options import PipelineOptions


class ImageExtract(beam.DoFn):

    def process(self, element, *args, **kwargs):
        from apache_beam.io.gcp import gcsio
        import zipfile

        # logging.info(repr(element))
        logging.info(element.path)
        with gcsio.GcsIO().open(element.path, 'r') as f:
            if zipfile.is_zipfile(f):
                logging.info('it is a zip file')
                z = zipfile.ZipFile(f)
                file_list = z.filelist
                for some_file in file_list:
                    if '.TIF' in some_file.filename:
                        logging.info( some_file.filename)
                        try:
                            data = z.read(some_file)
                        except KeyError:
                            logging.info('ERROR: Did not find %s in zip file' % some_file.filename)
                        else:
                            logging.info('zip file is : %s', some_file.filename)
                            outfile = 'gs://dataflow-buffer/python-4/' + some_file.filename
                            with gcsio.GcsIO().open(outfile, mode='w',
                                                    mime_type='image/tiff') as writing_path:
                                writing_path.write(data)
                                logging.info( 'write success : %s', some_file.filename)
        return


def run():
    p = beam.Pipeline(options=PipelineOptions())
    # gcs = GCSFileSystem(PipelineOptions())
    # 'gs://dataflow-buffer/parent-unpack/2018/i20180130/PxpFJwJabD-untarI20180130/DESIGN/USD0808610-20180130.ZIP'
    # input_pattern = ['gs://dataflow-buffer/parent-unpack/2018/i20180130/PxpFJwJabD-untar*/**/*.ZIP']
    input_pattern_1 = 'gs://dataflow-buffer/parent-unpack/2018/i20180130/PxpFJwJabD-untar*/**/*.ZIP'
    # result = [m.metadata_list for m in gcs.match(input_pattern)]

    (p
     | 'Match Files' >> fileio.MatchFiles(input_pattern_1)
     | 'Print read file' >> beam.ParDo(ImageExtract())
     )
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
