Data Coding Challenge
=======================
Design and create a Apache Spark job using this skeleton that will process a provided daily file and build a dataset calculating average movie ratings as of the latest file received and includes all previous votes.

Data
--------------
Data is in src/main/resources.

## Requirements
The operations we expect to see:
- Processing
  - Only the latest movie rating by a user should be used/counted.
  - Assume that daily files are ~2TB and lookup data is about 1MB.
  - Resultant dataset to be read performant/optimized for querying.
- Metadata join
  - Metadata can be updated at any time and the resultant dataset needs to only show the latest runtime and title.
  - Metadata may not always have the respective movie listed.
- Reprocessing
  - Reprocessing of any specific days, i.e. We should be able to reprocess any one day's file with minimally added performance hit.
- Operational
  - Job execution time, how long does the job take.
- Technologies used:
  - Scala
  - Apache Spark.


### Daily file format:
- Tab delimited file with the following columns:
  - user_id
  - movie_id
  - rating (1-5)
  - voting-timestamp (Epoch)

### Lookup dataset:
- Comma delimited file with the following columns:
  - movie_id
  - movie_runtime
  - movie_title


### Final output columns:
- movie_id
- movie_title
- movie_runtime
- avg(movie_rating)
- number_of_votes

## Checking Out the Project
The project is hosted here on Gitlab.com.

## Submitting the Project
You will also receive an invitation to your own private repository on gitlab.com. Push your code to that repository as
you go. Instructions are provided by gitlab.com on how to push your code to a new repo.

## Challenge Duration
Average time to complete this task is 2 hours.
