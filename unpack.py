from __future__ import absolute_import
import argparse
import logging
# import cloudstorage
import apache_beam as beam
from apache_beam.io.filesystem import FileSystem
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
from apache_beam.options.pipeline_options import PipelineOptions

# from google.cloud import vision
# from google.cloud.vision import types

# Specify default parameters.
INPUT_FILE = 'gs://python-dataflow-example/data_files/image-list.txt'
BQ_DATASET = 'ImageLabelFlow'
BQ_TABLE = BQ_DATASET + '.dogs_short'


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

        # try:
        #     with cloudstorage.open(filename) as cloudstorage_file:
        #         outfile = image_path
        #         success_output_path = 'https://storage.googleapis.com' + image_path
        #         try:
        #             img = images.Image(cloudstorage_file.read())
        #             img.im_feeling_lucky()
        #             result = img.execute_transforms(output_encoding=images.PNG)
        #             logging.info('converting file {}\n'.format(filename))
        #
        #             # The retry_params specified in the open call will override the default
        #             # retry params for this particular file handle.
        #             write_retry_params = cloudstorage.RetryParams(backoff_factor=1.1)
        #             with cloudstorage.open(
        #                     outfile, 'w', content_type='image/png',
        #                     retry_params=write_retry_params) as cloudstorage_file:
        #
        #                 cloudstorage_file.write(result)
        #                 cloudstorage_file.close()
        #                 self.success_files.append(success_output_path)
        #                 self.tmp_filenames_to_clean_up.append(filename)
        #         except IOError:
        #             logging.error("cannot convert", filename)
        #             self.copy_file(filename, image_path)
        #         except Exception as e:
        #             logging.error("error is", e)
        #             self.copy_file(filename, image_path)
        # except cloudstorage.NotFoundError as e:
        #     logging.error('file not found NotFoundError for :{}'.format(filename), e)

        return 'done'


def run():
    p = beam.Pipeline(options=PipelineOptions())
    gcs = GCSFileSystem(PipelineOptions())
    # 'gs://dataflow-buffer/parent-unpack/2018/i20180130/PxpFJwJabD-untarI20180130/DESIGN/USD0808610-20180130.ZIP'
    input_pattern = [
        'gs://dataflow-buffer/parent-unpack/2018/i20180130/PxpFJwJabD-untarI20180130/DESIGN/*.ZIP']

    result = [m.metadata_list for m in gcs.match(input_pattern)]
    (p
     | 'Read from a File' >> beam.Create(result)
     | 'Print read file' >> beam.ParDo(ImageLabeler())
     )
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
