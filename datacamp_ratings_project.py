# -*- coding: utf-8 -*-
"""
Created on Sun Feb 23 21:58:06 2020

PROJECT 1: DATACAMP COURSE RATINGS 

PROJECT SCOPE:
    get rating data
    clean and calculate top-recommended courses
    recalculate daily
    database -- datacamp_application
    
@author: Ricardo Redd
"""

#Querying the table:
#Complete the connection URI:
connection_uri = "postgresql://repl:password@localhost:5432/datacamp_application"
db_engine = sqlalchemy.create_engine(connection_uri)

#Get user with id 4387:
user1 = pd.readsql("SELECT * FROM rating WHERE user_id=4387", db_engine)

#Get user with id 18163:
user2 = pd.readsql("SELECT * FROM rating WHERE user_id=18163", db_engine)

#Get user with id 8770:
user3 = pd.readsql("SELECT * FROM rating WHERE user_id=8770", db_engine)

#Use the helper function to compare the 3 users:
print_user_comparison(user1, user2, user3)


#------Get Average rating per course------#
#Complete the transformation function:
def transform_avg_rating(rating_data):
    #Group by course_id and extract average rating per course:
    avg_rating = rating_data.groupby('course_id').rating.mean()
    #Return sorted average ratings per course
    sort_rating = avg_rating.sort_values(ascending=False).reset_index()
    return sort_rating

#Extract the rating data into a DataFrame
rating_data = extract_rating_data(db_engines)

#Use transform_avg_rating on the extracted data and print results:
avg_rating_data = transform_avg_rating(rating_data)
print(avg_rating_data)


# Filter out corrupt data -- no missing values should be in dataset:
course_data = extract_course_data(db_engines)

#Print out the number of missing values per column:
print(course_data.isnull().sum())

#The transformation should fill in the missing values:
def transform_fill_programming_language(course_data):
    imputed = course_data.fillna({"programming_language": "r"})
    return imputed

transformed = transform_fill_programming_language(course_data)

#Print out the number of missing values per column of transformed:
print(transformed.isnull().sum())

#******Using the recommender transformation******#

#Complete the transformation function:
def transform_recommendations(avg_course_ratings, courses_to_recommend):
    #Merge both DataFrames
    merged = courses_to_recommend.merge(avg_course_ratings)
    #Sort values by rating and group by user_id
    grouped = merged.sort_values("rating", ascending = False).groupby('user_id')
    #Produce the top 3 values and sort by user_id
    recommendations = grouped.head(3).sort_values("user_id").reset_index()
    final_recommendations = recommendations[["user_id", "course_id", "rating"]]
    #Return final recommendations:
    return final_recommendations

#Use the function with the predefined DataFrame objects:
recommendations = transform_recommendations(avg_course_ratings, courses_to_recommend)


#The Loading Phase:
recommendations.to_sql(
        "recommendations",
        db_engine,
        if_exists="append",
)

def etl(db_engines):
    #Extract the data
    courses = extract_course_data(db_engines)
    rating = extract_rating_data(db_engines)
    
    #Clean up courses data:
    courses = transform_fill_programming_language(courses)
    
    #Get the avg. course ratings:
    avg_course_rating = transform_avg_rating(rating)
    
    #Get eligible user and course id pairs:
    courses_to_recommend = transform_courses_to_recommend(
            rating, 
            courses,
)
    #Calculate the recommendations:
    recommendations = transform_recommendations(
            avg_course_rating,
            courses_to_recommend,
)
    
    #Load the recommendations into the database:
    load_to_dwh(recommendations, db_engine)
    

#Creating the DAG    
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG(dag_id="recommendations",
          schedule_interval="0 0 * * *")

task_recommendations = PythonOperator(
        task_id="recommendations_task",
        python_callable=etl,
)    

#--------------------------------EXERCISES---------------------------------
#The Target Table:
connection_uri = "postgresql://repl:password@Localhost:5432/dwh"
db_engine = sqlalchemy.create_engine(connection_uri)

def load_to_dwh(recommendations):
    recommendations.to_sql("recommendations", db_engine, if_exists="replace")
    
#Defining the DAG:
    
#Define the DAG so it runs on a daily basis:
dag = DAG(dag_id="recommendations",
          schedule_interval="0 0 * * *")

# Make sure `etl()` is called in the operator. Pass the correct kwargs:
task_recommendations = PythonOperator(
        task_id="recommendations_task",
        python_callable=etl,
        op_kwargs={"db_engines":db_engines},
)

#Querying the Recommendations:
def recommendations_for_user(user_id, threshold=4.5):
    
    #Join with the courses table:
    query = """
    SELECT title, rating FROM recommendations
      INNER JOIN courses ON courses.course_id = recommendations.course_id
      WHERE user_id=%(user_id)s AND rating>%(threshold)s
      ORDER BY rating DESC
    """
#Add the threshold parameter:
    predictions_df = pd.read_sql(query, db_engine, params = {"user_id":user_id,
                                                             "threshold": threshold})
    
    return predictions_df.title.values

#Try the functions just created:
print(recommendations_for_user(12, 4.65))























































