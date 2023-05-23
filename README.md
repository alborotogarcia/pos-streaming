# pos-streaming

PubSub -> Dataflow -> BigQuery

- Read from PubSub with 10 TimeWindows in Beam (Extract)
- Use beam coders to format json and extract lines from activity POS on the fly.
- Sink to BigQuery

## Set env vars

```bash
export PROJECT_ID = my-project
export GCP_REGION = "EU"
export STAGING_LOCATION = "gs://my-bucket/pos/stg"
export TEMP_LOCATION = "gs://my-bucket/pos/temp"
export TOPIC_NAME = my_topic
export DATASET_ID = dataset_id
export ACTIVITY_TABLE = activity_table
export LINE_TABLE = line_table
export RUNNER = DirectRunner
export TIME_WINDOW_MINUTES = 1.0
```
## Create local job
```bash
python activityline.py --project $PROJECT_ID --region $GCP_REGION --staging_location $STAGING_LOCATION --temp_location $TEMP_LOCATION --runner DirectRunner --output_line_table $DATASET_ID.$LINE_TABLE --output_activity_table $DATASET_ID.$ACTIVITY_TABLE --window_duration $TIME_WINDOW_MINUTES --input_topic projects/$PROJECT_ID/topics/$TOPIC_NAME
```
*To create a job in Dataflow just set env variable RUNNER to DataflowRunner*