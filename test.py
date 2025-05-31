import pyspark.sql.functions as fn
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as sf
from pyspark.sql.types import *
from os.path import abspath
import os, sys, csv, re, string, time, subprocess
import glob
from datetime import datetime
from pyspark.sql.functions import lit,unix_timestamp,length
from pyspark.sql.catalog import *
from datetime import datetime,date,timedelta
from pyspark.sql.functions import col
from pyspark.sql.functions import when
 
# Step 1: Initialize a Spark session
spark = SparkSession.builder \
    .appName("IPL 2025") \
    .getOrCreate()
    
df = spark.read.csv("file:///home/sisr6adm/analytics/userapplications/P55058192/_Projects_IPL_2025_/matches.csv", header = True, inferSchema=True)

# df.show(100, False)

# show data types of all df columns 

df.printSchema()

# drop not required columns

df = df.drop("wb_runs","wb_wickets")

df.show(100, truncate=False)

# Matches count

print("Total Match Count:", df.count())


# Data cleaning and handle missing values and Replace missing values with 0 using below function for 1 column

# df.na.fill(value=0,subset=["first_ings_score"]).show(100, False)

# Convert string column to integer

# for multiple integer columns use below syntax

# df.na.fill(value=0, subset=["col1", "col2"])

df = df.na.fill(value=0, subset=["first_ings_score", "first_ings_wkts", "second_ings_score","second_ings_wkts","balls_left","highscore"])

df.show(100, False)

print("Total Match Count:", df.count())

# Unique teams from Team1 and Team 2

df.select("team1").distinct().show()

df.select("team2").distinct().show()

# Most toss winner or not Team

df.select(max("toss_winner").alias("max_toss_winner")).show()

df.select(min("toss_winner").alias("min_toss_looser")).show()

# highest score in first inngs

df.select(max("first_ings_score").alias("max_first_innings_score")).show()

# lowest score in first inngs

df.select(min("first_ings_score").alias("min_first_innings_Score")).show()

# highest score in first inngs

df.select(max("second_ings_score").alias("max_second_innings_score")).show()

# lowest score in first inngs

df.select(min("second_ings_score").alias("min_second_innings_Score")).show()

# Team highest score in first inngs

# Add a new column to identify first innings team
df_with_first_batting = df.withColumn(
    "first_innings_team",
    when(df["toss_decision"] == "Bowl",
         when(df["toss_winner"] == df["team1"], df["team2"])
    ).otherwise(df["toss_winner"])
)

# Find team with max first innings score
max_score = df_with_first_batting.agg(max("first_ings_score")).collect()[0][0]

df_with_first_batting.filter(df_with_first_batting["first_ings_score"] == max_score).select(
    "match_id", "team1", "first_ings_score"
).show()


# Team loweest score in first inngs

# Add a new column to identify first innings team
df_with_first_batting = df.withColumn(
    "first_innings_team",
    when(df["toss_decision"] == "Bowl",
         when(df["toss_winner"] == df["team1"], df["team2"])
    ).otherwise(df["toss_winner"])
)

# Find team with min first innings score
min_score = df_with_first_batting.agg(min("first_ings_score")).collect()[0][0]

df_with_first_batting.filter(df_with_first_batting["first_ings_score"] == min_score).select(
    "match_id", "team1", "first_ings_score"
).show()

# Team which did highest score in second inngs

# Add a new column to identify first innings team
df_with_first_batting = df.withColumn(
    "second_innings_team",
    when(df["toss_decision"] == "Bowl",
         when(df["toss_winner"] == df["team1"], df["team2"])
    ).otherwise(df["toss_winner"])
)

# Find team with max first innings score
max_score = df_with_first_batting.agg(max("second_ings_score")).collect()[0][0]

df_with_first_batting.filter(df_with_first_batting["second_ings_score"] == max_score).select(
    "match_id", "team1", "second_ings_score"
).show()

# Team which did lowest score in second inngs

# Add a new column to identify first innings team
df_with_first_batting = df.withColumn(
    "second_innings_team",
    when(df["toss_decision"] == "Bowl",
         when(df["toss_winner"] == df["team1"], df["team2"])
    ).otherwise(df["toss_winner"])
)

# Find team with max first innings score
min_score = df_with_first_batting.agg(min("second_ings_score")).collect()[0][0]

df_with_first_batting.filter(df_with_first_batting["second_ings_score"] == min_score).select(
    "match_id", "team1", "second_ings_score"
).show()


# Find teams which tied by comparing 1 and 2 innings 

df1 = df.filter((df["first_ings_score"] == df["second_ings_score"]) & (df["match_result"] == 'tied'))
df1.show(60, False) # Team which tied and did superover


df2 = df.filter(df["first_ings_score"] > df["second_ings_score"])
df2.show(60, False)   # Team batting first won

df3 = df.filter(df["first_ings_score"] < df["second_ings_score"])
df3.show(60, False)   # Team chasing won

# Find who is most top_scorer

top_scorer_df = df.groupBy("top_scorer").agg(count("*").alias("count")).orderBy(col("count").desc())
top_scorer_df.show(60, False)

# top_scorer_df.show(1, False)  # Show top 1 score

# Find who is most player of Matches

player_of_match_df = df.groupBy("player_of_the_match").agg(count("*").alias("count")).orderBy(col("count").desc())
player_of_match_df.show(60, False)

# player_of_match_df.show(1, False)  # Show top 1 score

# Find which team wins most Matches

most_wins_df = df.groupBy("match_winner") \
                 .agg(count("*").alias("win_count")) \
                 .orderBy(col("win_count").desc())
                 
most_wins_df.show()

df_cleaned = df.na.drop(subset=["match_winner"])

# Find which team looes most Matches

# Add losing_team column
df_with_loser = df.withColumn(
    "losing_team",
    when(col("match_winner") == col("team1"), col("team2"))
    .when(col("match_winner") == col("team2"), col("team1"))
)

# Count number of times each team lost
losing_counts = df_with_loser.groupBy("losing_team") \
                             .agg(count("*").alias("loss_count")) \
                             .orderBy(col("loss_count").desc())

losing_counts.show()


# Total runs by team

# Create two dataframes: one for first innings, one for second innings
first_ings = df.select(col("team1").alias("team"), col("first_ings_score").alias("runs"))
second_ings = df.select(col("team2").alias("team"), col("second_ings_score").alias("runs"))

# Union both innings
all_runs = first_ings.union(second_ings)

# Group by team and sum runs
total_runs_by_team = all_runs.groupBy("team").agg(sum("runs").alias("total_runs"))

# Show results
total_runs_by_team.orderBy(col("total_runs").desc()).show()

# Group by venue and count the number of matches played at each venue
most_venue_df = df.groupBy("venue") \
                  .agg(count("*").alias("match_count")) \
                  .orderBy(col("match_count").desc())

# Show the venue with the most matches

# PySpark’s .show() method truncates strings by default to keep the console output neat and readable, especially when you have very long strings or wide tables.

most_venue_df.show(1)  # show top 1

most_venue_df.show()  # show all

# By using below method : truncate=False	No truncation	Show full strings regardless of length

most_venue_df.show(n=1, truncate=False)  # show top 1

most_venue_df.show(truncate=False)  # show all


# Find which team left max balls_left

balls_left_df = df.groupBy("team2") \
                  .agg(count("balls_left").alias("balls_left_count")) \
                  .orderBy(col("balls_left_count").desc())
                  
balls_left_df.show()                  


# Find which team did most bat first

# Create a new column 'team_batted_first'
df = df.withColumn(
    "team_batted_first",
    when(col("toss_decision") == "Bat", col("toss_winner"))
    .otherwise(when(col("team1") != col("toss_winner"), col("team1")).otherwise(col("team2")))
)

# Show the updated DataFrame
df.select("match_id", "team1", "team2", "toss_winner", "toss_decision", "team_batted_first").show(60, False)

# Group by 'team_batted_first' and count the occurrences
result = df.groupBy("team_batted_first").count().orderBy("count", ascending=False)

# Show the result
result.show()


# Find whcih team did most bowl first

# Create a new column 'team_batted_first'
df = df.withColumn(
    "team_bowled_first",
    when(col("toss_decision") == "Bowl", col("toss_winner"))
    .otherwise(when(col("team1") != col("toss_winner"), col("team1")).otherwise(col("team2")))
)

# Show the updated DataFrame
df.select("match_id", "team1", "team2", "toss_winner", "toss_decision", "team_bowled_first").show(60, False)

# Group by 'team_batted_first' and count the occurrences
result = df.groupBy("team_bowled_first").count().orderBy("count", ascending=False).na.drop(subset=["team_bowled_first"])

# Show the result
result.show()



# spark.stop()