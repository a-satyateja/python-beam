from __future__ import absolute_import
# import argparse
import logging
import zipfile
import apache_beam as beam
# from google import auth
from apache_beam.io.gcp import gcsio
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
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


class ImageLabeler(beam.DoFn):
    """
    This DoFn wraps the inner function detect_labels_uri to write a BigQuery row dictionary
    with the image file reference specified by the element from the prior PCollection and a list
    of labels for that image assigned by the Google Cloud Vision API.
    """

    def process(self, element, *args, **kwargs):
        """
        Args:
            element: A string specifying the uri of an image
        Returns:
            row: A list containing a dictionary defining a record to be written to BigQuery
        """
        print(element)
        # print(element[0].path)
        # file_path = element[0].path
        logging.info(repr(element))
        for el in element:
            with beam.io.gcp.gcsio.GcsIO().open(el.path, 'r') as f:
                if zipfile.is_zipfile(f):
                    logging.info('it is a zip file')
                    z = zipfile.ZipFile(f)
                    file_list = z.filelist
                    # print file_list[0].filename
                    for some_file in file_list:
                        if '.TIF' in some_file.filename:
                            print some_file.filename
                            try:
                                data = z.read(some_file)
                            except KeyError:
                                print 'ERROR: Did not find %s in zip file' % some_file
                            else:
                                print some_file.filename, ':'
                                outfile = 'gs://dataflow-buffer/python-1/' + some_file.filename
                                with beam.io.gcp.gcsio.GcsIO().open(outfile, mode='w',
                                                                    mime_type='image/tiff') as writing_path:
                                    writing_path.write(data)
                                    writing_path.close()
                                    print 'write success'
        return 'done'


def run():
    p = beam.Pipeline(options=PipelineOptions())
    gcs = GCSFileSystem(PipelineOptions())
    # 'gs://dataflow-buffer/parent-unpack/2018/i20180130/PxpFJwJabD-untarI20180130/DESIGN/USD0808610-20180130.ZIP'
    input_pattern = [
        'gs://dataflow-buffer/parent-unpack/2018/i20180130/PxpFJwJabD-untar*/**/*.ZIP']

    result = [m.metadata_list for m in gcs.match(input_pattern)]
    (p
     | 'Read from a File' >> beam.Create(result)
     | 'Print read file' >> beam.ParDo(ImageLabeler())
     )
    p.run().wait_until_finish()


# def get_service_credentials():
#     """For internal use only; no backwards-compatibility guarantees.
#
#     Get credentials to access Google services.
#
#     Returns:
#       A ``google.auth.credentials.Credentials`` object or None if credentials not
#       found. Returned object is thread-safe.
#     """
#     return _Credentials.get_service_credentials()
#
#
# class _Credentials(object):
#     _credentials_lock = beam.threading.Lock()
#     _credentials_init = False
#     _credentials = None
#
#     @classmethod
#     def get_service_credentials(cls):
#         if cls._credentials_init:
#             return cls._credentials
#
#         client_scopes = [
#             'https://www.googleapis.com/auth/bigquery',
#             'https://www.googleapis.com/auth/cloud-platform',
#             'https://www.googleapis.com/auth/devstorage.full_control',
#             'https://www.googleapis.com/auth/userinfo.email',
#             'https://www.googleapis.com/auth/datastore'
#         ]
#         try:
#             with cls._credentials_lock:
#                 if cls._credentials_init:
#                     return cls._credentials
#                 cls._credentials, project_id = auth.default(client_scopes)
#                 # if is_running_in_gce:
#                 #     assert project_id == executing_project
#                 cls._credentials_init = True
#                 # TODO: remove?
#                 logging.info('Got credentials %r for project: %s', cls._credentials, project_id)
#         except auth.exceptions.DefaultCredentialsError as e:
#             logging.warning(
#                 'Unable to find default credentials to use: %s\n'
#                 'Connecting anonymously.', e)
#             return None
#         return cls._credentials


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
