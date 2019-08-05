from __future__ import absolute_import
import logging
from apache_beam.io import fileio
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# from google.cloud import vision
# from google.cloud.vision import types

# Specify default parameters.

# def detect_labels_uri(uri):
#     """This Function detects labels in the image file located in Google Cloud Storage or on
#     theWeb and returns a comma separated list of labels. This will return an empty string
#     if not passed a valid image file
#     Args:
#         uri: a string link to a photo in gcs or on the web
#     (Adapted From:
#     https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/vision/cloud-client/detect/detect.py)
#     """
#     # Initialize cloud vision api client and image object.
#     client = vision.ImageAnnotatorClient()
#     image = types.Image()
#     image.source.image_uri = uri
#
#     # Send an api call for this image, extract label descriptions
#     # and return a comma-space separated string.
#     response = client.label_detection(image=image)
#     labels = response.label_annotations
#     label_list = [l.description for l in labels]
#     return ', '.join(label_list)


class ExtractFiles(beam.DoFn):
    """
    This DoFn wraps the inner function detect_labels_uri to write a BigQuery row dictionary
    with the image file reference specified by the element from the prior PCollection and a list
    of labels for that image assigned by the Google Cloud Vision API.
    """

    def process(self, element, *args, **kwargs):
        from apache_beam.io.gcp import gcsio
        import zipfile
        """
        Args:
            element: A string specifying the uri of an image
        Returns:
            row: A list containing a dictionary defining a record to be written to BigQuery
        """

        # file_path = element[0].path
        logging.info(repr(element))
        # print(element[0].path)
        # print(len(element))
        # for el in element:
        with gcsio.GcsIO().open(element.path, 'r') as f:
            if zipfile.is_zipfile(f):
                logging.info('it is a zip file')
                z = zipfile.ZipFile(f)
                file_list = z.filelist
                # print file_list[0].filename
                for some_file in file_list:
                    if '.TIF' in some_file.filename:
                        print some_file.filename
                        logging.info('zip file is : %s', some_file.filename)
                        try:
                            data = z.read(some_file)
                        except KeyError:
                            print 'ERROR: Did not find %s in zip file' % some_file
                        else:
                            print some_file.filename, ':'
                            outfile = 'gs://dataflow-buffer/python-2/' + some_file.filename
                            with gcsio.GcsIO().open(outfile, mode='w',
                                                    mime_type='image/tiff') as writing_path:
                                writing_path.write(data)
                                writing_path.close()
                                print 'write success'
        return 'done'


def run():
    p = beam.Pipeline(options=PipelineOptions())
    # 'gs://dataflow-buffer/parent-unpack/2018/i20180130/PxpFJwJabD-untarI20180130/DESIGN/USD0808610-20180130.ZIP'
    input_pattern = 'gs://dataflow-buffer/parent-unpack/2018/i20180130/PxpFJwJabD-untar*/**/*.ZIP'

    (p
     | 'Match Files' >> fileio.MatchFiles(input_pattern)
     | 'Extract Files' >> beam.ParDo(ExtractFiles())
     )
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
