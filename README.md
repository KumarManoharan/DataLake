# Project: Data Lake

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Project Description

As their data engineer, I have built an ETL pipeline that extracts their data from S3, transforms he data and loads them to partitioned parquet files in AWS S3 into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

## Project Objective

The project delivers a partitioned parquet files Data Warehouse in table directories on AWS S3 that is built based on the Sparkify data that can be used by the Sparkify data analytics team to perform their analysis by directly running the queries or use a BI tool.

## Project Datasets

### 1. Song Dataset

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. 

### 2. Log Dataset

The second dataset consists of log files in JSON format. The log files in the dataset with are partitioned by year and month. 

## Schema for Song Play Analysis

A Star Schema has been created as required for optimized queries on song play queries, with parioning as below:

### Fact Table

**songplays** - records in event data associated with song plays i.e. records with page NextSong 
                songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent (Partioned by "month" and "year")

### Dimension Tables

**users**   - users in the app 
              user_id, first_name, last_name, gender, level

**songs**   - songs in music database 
              song_id, title, artist_id, year, duration (Partioned by "year", "artist_id")

**artists** - artists in music database 
              artist_id, name, location, lattitude, longitude 

**time**    - timestamps of records in songplays broken down into specific units 
              start_time, hour, day, week, month, year, weekday (Partioned by "month" and "year")

## Project Files

1. etl.py    - Reads data from S3, processes that data using Spark and writes them back to S3

2. dl.cfg    - Rontains AWS Credentials such as Access Key ID and Secret access key

3. README.md - Provides discussion about the ETL process, briefing various aspects of this project.

## ETL Pipeline

1. Load the AWS credentials from dl.cfg

2. Using SPARK extract Song Data and Log Data JSON Files from S3, perform the required transform actions on the data and load them into the partitioned parquet files in table directories on S3.

The whole ETL process can ben exectued by running the etl.py file from the terminal by using the command **python etl.py**