# How to use vision service?

## Pre-requisites
* Create a GCP project. [Learn more](https://cloud.google.com/vision/docs/setup#project)
* Enable [Cloud Vision Api](https://console.cloud.google.com/flows/enableapi?apiid=vision.googleapis.com&_ga=2.227739997.1707494369.1681768352-665004272.1681768352)
* Create two GCS buckets. [Learn more](https://cloud.google.com/storage/docs/creating-buckets)

## Steps to run
* Import external vision service from vision_service library.

```py
from vision_service import ExternalVisionService
```

* Define features using python's dictionary. Check available features [here](https://cloud.google.com/vision/docs/reference/rest/v1/Feature#type)

```py
features={'feature_name': <max_results>, 'feature_name': <max_results>}
```
Note: `max_results` is maximum number of results of given type. Does not apply to TEXT_DETECTION, DOCUMENT_TEXT_DETECTION, or CROP_HINTS.

* Define ExternalVisionService object.

```py
service_obj = ExternalVisionService(
      input_gcs_uri='<your-gcs-bucket-uri>',
      output_gcs_uri='<your-gcs-bucket-uri>',
      features=features,
  )
```

* Call `run_async_batch_annotate_images` to get insights from your images.

```py
service_obj.run_async_batch_annotate_images(batch_size=<int>, time_to_sleep=<int>)
```
`batch_size`: It is an option to get response in aggregated manner. For ex, if 4 images are there in your input bucket and you set batch_size=2 then it will create 2 output files that will have responses of all 4 images.

`time_to_sleep` : Time to wait in seconds for checking status of the job or operation.

* Done. Results should be populated to your output bucket.


