from pyspark.sql import SparkSession

# SparkSQL을 이용하여 평점이 가장 낮은 10개의 영화 찾기 
if __name__ == "__main__":
    spark = SparkSession.builder.appName("MostEval").getOrCreate()

    df1 = spark.read.load("hdfs://user/maria_dev/ml-latest-small/ratings.csv",
    format="csv",sep=",",inferSchema="true",header="true")

    df2 = spark.read.load("hdfs://user/maria_dev/ml-latest-small/movies.csv",
    format="csv",sep=",",inferSchema="true",header="true")



    df1.createOrReplaceTempView("ratings")
    df1.createOrReplaceTempView("movies")

    result= spark.sql("""
    SELECT title, score 
    FROM movies JOIN (
        SELECT movieId,avg(rating) as score 
        FROM ratings GROUP BY movieId 
        ) r ON movies.movieId = r.movieId
        ORDER BY score LIMIT 10 
    """)

    for row in result.collect():
        print(row.title,row.score)  # title과 score를 한줄씩 출력  
