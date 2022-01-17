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
S3-DATA-LAKE-WITH-SPARK-ETL/
│
├── README.md
├── data/ 
    ├── log-data/
    ├── song_data/ 
├── dl.cfg -->config file with all necessary details to run on aws
├── etl.py --> script reads data from S3, transforms them to five tables, and writes them to S3
├── requirements.txt
├── prototype/
    ├── dev_notebook.ipynb --> jupyter notebook for developing the code which later was moved to app/
├── LICENSE


```
## How To Run<a name="execution"></a>

- STEP 1: Create a virtual environment with Python 3.8 using the requirements.txt file in this project.

- Step 2: Create a dl.cfg file with the following structure containing your AWS credentials (make sure to not include your AWS access keys in your code when sharing this project!):
```
[AWS]
AWS_ACCESS_KEY_ID='<your access key>'
AWS_SECRET_ACCESS_KEY='<your secret key>'

[S3]
SOURCE_S3_BUCKET ='s3a://udacity-dend/'
DEST_S3_BUCKET =''
```
- STEP 3: Either run the dev_notebook.ipynb or run the etl.py file in the terminal.

## Data Model<a name="model"></a>

- Fact Table
  - songplays - records in log data associated with song plays i.e. records with page NextSong
    - songplay_id, 
    - start_time, 
    - user_id, level, 
    - song_id, 
    - artist_id, 
    - session_id, 
    - location, 
    - user_agent
  
- Dimension Tables
  - users - users in the app
    - user_id, 
    - first_name, 
    - last_name, 
    - gender, level
  - songs - songs in music database
    - song_id, 
    - title, 
    - artist_id, 
    - year, 
    - duration 
  - artists - artists in music database
    - artist_id, 
    - name, 
    - location, 
    - lattitude, 
    - longitude
  - time - timestamps of records in songplays broken down into specific units
    - start_time, 
    - hour, 
    - day, 
    - week, 
    - month, 
    - year, 
    - weekday

## Licensing, Authors, Acknowledgements<a name="licensing"></a>

I give credit to Udacity for the data.

Feel free to use my code as you please:

Copyright 2021 Leopold Walther

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.