# s3-data-lake-with-spark-ETL

### Table of Contents

1. [Introduction](#intro)
2. [Project Details](#details)
3. [File Descriptions](#files)
4. [How To Run](#execution)
5. [Data Model](#model)
6. [Licensing, Authors, and Acknowledgements](#licensing)

## Introduction<a name="intro"></a>

This project is part of the Udacity Nanodegree "Data Engineer".
Goal of this project is to build an ETL pipeline for a data lake hosted on S3. First step is to load data from S3, then process the data into analytics tables using Spark, and after that load them back into S3. This Spark process will be deployed on a cluster using AWS.

## Project Details<a name="details"></a>

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The task is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## File Descriptions <a name="files"></a>

Here you can see the files of the project with a short description:

```
AWS-S3-TO-REDSHIFT-ETL_PIPELINE/
│
├── README.md
├── requirements.txt
├── dl.cfg -->config file with all necessary details to run cluster on aws
├── app/ 
    ├── 
    ├── 
├── prototype/
    ├── dev_notebook.ipynb --> jupyter notebook for developing the code which later was moved to app/
├── LICENSE


```
## How To Run<a name="execution"></a>

- STEP 1:  
  
- Step 2: 


## Data Model<a name="model"></a>


## Results<a name="results"></a>


## Licensing, Authors, Acknowledgements<a name="licensing"></a>

I give credit to Udacity for the data.

Feel free to use my code as you please:

Copyright 2021 Leopold Walther

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.