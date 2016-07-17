# ProfileTrimming
This is my hobby project. The objective is to find proper method to shrink  user profiles  so less amount of disk space will be consumed.
As a case-study, I am looking at the CF Recommender Systems, where user profiles mainly accommodates ratings.

The current solution relies on Spark ML library, and considers both user-based and item-based CF recommenders. For user based I use Spark implementaion ALS and for items based I end up developing my own approach.

The implementaton still is not compuationally optimized.
