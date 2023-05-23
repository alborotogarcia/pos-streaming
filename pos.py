import time
import apache_beam as beam
import typing
from apache_beam.utils.timestamp import Timestamp
import json
from datetime import datetime
import argparse
import logging
import random
from uuid6 import uuid7 as uuid
from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys, FlatMap, Map
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows, GlobalWindows
from apache_beam.runners import DataflowRunner, DirectRunner
from apache_beam.options.pipeline_options import GoogleCloudOptions, PipelineOptions, StandardOptions

class LineLog(typing.NamedTuple):
    id:	                int = None
    product_id:	        int = None
    quantity:	        float = None
    price:	            float = None
    custom2:	        str = ' '
    custom3:	        str = ' '
    custom4:	        str = ' '
    custom5:	        int = ' '
    custom6:	        str = ' '
    custom7:	        str = ' '
    custom8:	        str = ' '
    custom9:	        str = ' '
    created_at:	        str = Timestamp
    updated_at:	        bool = Timestamp
    size:	            str = None
    color:	            str = None
    discount:	        float = None
    discount_type:	    str = None
    product_code:	    str = None
    total:	            float = None
    company_id:	        int = None
    activity_code:	    str = None
    weight:	            int = None
    description:	    str = None
    pending_code_sync:	bool = None
    total_cost:	        float = None
    total_tax:	        float = None
    product_ean:	    str = None

class ActivityLog(typing.NamedTuple):
    id:	                int = None
    customer_id:	    int = None
    location_id:	    int = None
    company_id:	        int = None
    created_at:	        Timestamp = None
    updated_at:	        Timestamp = None
    name:	            str = None
    day_week:	        int = None
    session:	        Timestamp = None
    total:	            float = None
    code:	            str = None
    processed:	        bool = None
    returned:	        bool = None
    barcode:	        str = None
    customer_code:	    str = None
    location_code:	    str = None
    staff_code:	        str = None
    custom1:	        str = ' '
    pending_code_sync:	bool = None
    points:	            int = None
    returned_at:	    str = None
    synchronised_at:	Timestamp = None
    profile_id:	        int = None
    custom2:	        str = ' '
    custom3:	        str = ' '
    custom4:	        str = ' '
    custom5:	        str = ' '
    custom6:	        str = ' '
    custom7:	        str = ' '
    custom8:	        str = ' '
    total_tax:	        float = None
    associate_id:	    float = None
    ingest_datetime:	Timestamp = None
    uuid:	        	str = None

beam.coders.registry.register_coder(LineLog, beam.coders.RowCoder)
beam.coders.registry.register_coder(ActivityLog, beam.coders.RowCoder)
lineKeys = list(filter(lambda key: not key.startswith("_"),LineLog.__dict__.keys()))
activityKeys = list(filter(lambda key: not key.startswith("_"),ActivityLog.__dict__.keys()))

with open ('schemas/activity_table_schema.json') as schema:
    activity_table_schema = json.load(schema)
    activity_table_schema=','.join([f'{field["name"]}:{field["type"]}' for field in activity_table_schema['fields']])


with open ('schemas/lines_table_schema.json') as schema:
    lines_table_schema = json.load(schema)
    lines_table_schema=','.join([f'{field["name"]}:{field["type"]}' for field in lines_table_schema['fields']])

additional_bq_parameters = {
  'timePartitioning': {'field':'created_at','type': 'DAY'},
#   'clustering': {'fields': ['country']}
  }


class GroupMessagesByFixedWindows(PTransform):
    """A composite transform that groups Pub/Sub messages based on publish time
    and outputs a list of tuples, each containing a message and its publish time.
    """
    def __init__(self, window_size, num_shards=5):
        # Set window size to 60 seconds.
        self.window_size = int(window_size * 60)
        self.num_shards = num_shards
    def expand(self, pcoll):
        return (
            pcoll
            # Bind window info to each element using element timestamp (or publish time).
            | "Window into fixed intervals" >> WindowInto(FixedWindows(self.window_size))
            # | "Add timestamp to windowed elements" >> ParDo(AddTimestamp())
            # Assign a random key to each windowed element based on the number of shards.
            | "Add key" >> WithKeys(lambda _: random.randint(0, self.num_shards - 1))
            # Group windowed elements by key. All the elements in the same window must fit
            # memory for this. If not, you need to use `beam.util.BatchElements`.
            | "Group by key" >> GroupByKey()
            | "Abandon Dummy Key" >> beam.MapTuple(lambda _, val: val)
        )


class MapLineLog(beam.DoFn):
    def process(self, element, publish_time=beam.DoFn.TimestampParam):
        logging.getLogger().setLevel(logging.INFO)
        """Processes each incoming windowed element by extracting the Pub/Sub
        message and its publish timestamp into a dictionary. `publish_time`
        defaults to the publish timestamp returned by the Pub/Sub server. It
        is bound to each element by Beam at runtime.
        """
        try:
            lines = json.loads(element.decode("utf-8"))['lines']
            for line in lines:
                # drop keys not in LinesLog class
                popKeys = set(filter(lambda key: key not in lineKeys,line.keys()))
                for i in range(1,9):
                    line[f'custom{i}'] = ' ' or str(line[f'custom{i}'])
                message = dict([(k, v) for k,v in line.items() if k not in popKeys])
                addKeys = set(filter(lambda key: key not in line.keys(),lineKeys))
                # message.update(dict.fromkeys(addKeys))  
                yield LineLog(**{**message,**dict.fromkeys(addKeys)})#._asdict()
        except (ValueError, AttributeError) as e:
            # logging.info(f"[Invalid Data] ({e}) - {element}")
            pass

class MapActivityLog(beam.DoFn):
    def process(self, element, publish_time=beam.DoFn.TimestampParam):
        logging.getLogger().setLevel(logging.INFO)
        """Processes each incoming windowed element by extracting the Pub/Sub
        message and its publish timestamp into a dictionary. `publish_time`
        defaults to the publish timestamp returned by the Pub/Sub server. It
        is bound to each element by Beam at runtime.
        """
        try:
            message = json.loads(element.decode("iso-8859-1"))
            message["ingest_datetime"] = datetime.utcfromtimestamp(
                time.time()
            ).strftime("%Y-%m-%d %H:%M:%S.%f")
            message['uuid'] = str(uuid())
            # drop keys not in ActivityLog class
            popKeys = set(filter(lambda key: key not in activityKeys,message.keys()))
            for i in range(1,9):
                message[f'custom{i}'] = ' ' or str(message[f'custom{i}'])
            message = dict([(k, v) for k,v in message.items() if k not in popKeys])
            addKeys = set(filter(lambda key: key not in message.keys(),activityKeys))
            # message.update(dict.fromkeys(addKeys))  
            yield ActivityLog(**{**message,**dict.fromkeys(addKeys)})#._asdict()
        except (ValueError, AttributeError) as e:
            logging.info(f"[Invalid Data] ({e}) - {element}")
            pass



def run(argv=None, save_main_session=True):
    """Build and run the pipeline."""
    parser = argparse.ArgumentParser(description='Load from Json from Pub/Sub into BigQuery')

    # Google Cloud options
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')#, default='my-project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')#, default='EU')
    parser.add_argument('--staging_location', required=True, help='Specify Cloud Storage bucket for staging')#, default='gs://my-bucket/stg')
    parser.add_argument('--temp_location', required=True, help='Specify Cloud Storage bucket for temp')#, default='gs://my-bucket/temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner', default='DataflowRunner') #default='InteractiveRunner')

    # Pipeline specific options
    parser.add_argument('--output_line_table',default='dataset.line_table',
        help=(
            'Input BigQuery Line table'
            '"projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION>."'))
    parser.add_argument('--output_activity_table',default='dataset.activity_table',
        help=(
            'Output BigQuery Activity table'
            '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
    parser.add_argument('--window_duration', required=True, help='Window duration in minutes')
    # parser.add_argument('--num_shards', required=True, help='Number of shards')
    # parser.add_argument('--allowed_lateness', required=True, help='Allowed lateness')
    # parser.add_argument('--dead_letter_bucket', required=True, help='GCS Bucket for unparsable Pub/Sub messages')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--input_topic',default='projects/my-project/topics/my-topic',
        help=(
            'Input PubSub topic of the form '
            '"projects/<PROJECT>/topics/<TOPIC>".'))
    group.add_argument('--input_subscription',
        help=(
            'Input PubSub subscription of the form '
            '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Setting up the Beam pipeline options
    options = PipelineOptions(pipeline_args, save_main_session=True, streaming=True)
    options.view_as(GoogleCloudOptions).project = known_args.project
    options.view_as(GoogleCloudOptions).region = known_args.region
    options.view_as(GoogleCloudOptions).staging_location = known_args.staging_location
    options.view_as(GoogleCloudOptions).temp_location = known_args.temp_location
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('pos-minute-pipeline--',time.time_ns())
    options.view_as(StandardOptions).runner = known_args.runner

    # input_topic = opts.input_topic
    line_table = known_args.output_line_table
    activity_table = known_args.output_activity_table
    # window_duration = known_args.window_duration
    window_duration = 1.0
    # allowed_lateness = known_args.allowed_lateness
    # dead_letter_bucket = known_args.dead_letter_bucket
    # output_path = dead_letter_bucket + '/deadletter/'
    num_shards=5

    with Pipeline(options=options) as p:
        # events = pipeline | "Read from Pub/Sub" >> io.ReadFromPubSub(topic=input_topic)#,id_label=)

        # Read from PubSub into a PCollection.
        if known_args.input_subscription:
            messages = (
                p
                | beam.io.ReadFromPubSub(subscription=known_args.input_subscription).
                with_output_types(bytes))
        else:
            messages = (
                p
                | beam.io.ReadFromPubSub(
                    topic=known_args.input_topic).with_output_types(bytes))
        windowed_lines = (
            messages
            # Because `timestamp_attribute` is unspecified in `ReadFromPubSub`, Beam
            # binds the publish time returned by the Pub/Sub server for each message
            # to the element's timestamp parameter, accessible via `DoFn.TimestampParam`.
            # https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.pubsub.html#apache_beam.io.gcp.pubsub.ReadFromPubSub
            | "Map to LineLog" >> ParDo(MapLineLog().with_output_types(LineLog))
            | f"Window lines into {window_duration} minutes" >> GroupMessagesByFixedWindows(window_duration, num_shards)

        )
        (
            windowed_lines 
            # | "Re-window" >> beam.WindowInto(GlobalWindows())
            # | "Combine-window" >> beam.CombineGlobally(json.dumps)
            | 'Flatten line lists' >> FlatMap(lambda elements: elements)
            | 'Line serialization' >> Map(lambda element: element._asdict())
            # | "Print" >> Map(print)
            | "Write Line To BigQuery" >> io.WriteToBigQuery(
                line_table,
                schema=lines_table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                additional_bq_parameters=additional_bq_parameters
            )
        )
        windowed_activities = (
            messages
            | "Map to Activity" >> ParDo(MapActivityLog().with_output_types(ActivityLog))
            | f"Window activities into {window_duration} minutes" >> GroupMessagesByFixedWindows(window_duration, num_shards)

        )
        (
            windowed_activities 
            # | "Re-window" >> beam.WindowInto(GlobalWindows())
            | 'Flatten activity lists' >> FlatMap(lambda elements: elements)
            | 'Activity serialization' >> Map(lambda element: element._asdict())
            # | "Print" >> Map(print)
            | "Write Activity To BigQuery" >> io.WriteToBigQuery(
                activity_table,
                schema=activity_table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                additional_bq_parameters=additional_bq_parameters
            )
        )


    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run().wait_until_finish()

if __name__ == '__main__':
  run()