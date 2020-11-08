## Udacity Data Engineering NanoDegree Project 4: Data Lakes

#### Rubric for this project can be found here: https://review.udacity.com/#!/rubrics/2502/view

#### This project continues to follow the imaginary startup Sparkify as they scale their data infrastructure. They have reached such a high volume of data and have requested to migrate it to a Data Lake on AWS.

## Definition of Working Files

#### This repository contains one python script named <code>etl.py</code>. It can be run from the command line preceeded by 'python' like so: <code>python etl.py</code>. The <code>dl.cfg</code> file contains the appropriate AWS credentials.

#### The script pulls all Sparkify's data from S3, transforms it into efficient file formats and tables, then loads the data back into S3.

## Technologies Used

* <code>Spark</code> was the best option due to its ability to handle the data volume and speed requirements.

* <code>Python</code> was used as the primary language due to the various packages available to it and my familiarity with it.
