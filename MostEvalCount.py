from pyspark import SparkConf,sparkContext
from itertools import islice
import csv

# 평점이 가장 낮은 10개의 영화 찾기 


def loadMovies():
    movies = {}    
    with open("/home/maria_dev/data/ml-latest-small/movies.csv","rb") as f: 
        reader =csv.reader(f,delimiter=',')
        next(reader)       # 첫줄 건너뛰기 
        for row in reader : 
            movies[int(row[0])]=row[1]  # { movieId , title } pair 쌍이 movies에 여러개 저장 
        return movies 

def parseInput(line):
    fields = line.split(',')
    return (int(fields[1]),float(fields[2],1.0)) #  영화 id , ( 평점 , 평점데이터개수 ) 를 반환 


if __name__=="__main__":
    movies=loadMovies()  # movie.csv의 데이터 가져오기 - movies에는 { movieId , title } 이 저장되어있음 
    path="hdfs://user/maria_dev/ml-latest-small/ratings.csv" 

    conf=SparkConf().setAppName("MostEvalCount")
    sc=sparkContext(conf=conf)


    lines=sc.textFile(path) # 텍스트 파일로부터 RDD 생성 hdfs에서 읽어서 RDD가 만들어짐 

    #첫줄 생략 하는 코드 
    lines=lines.mapPartitionsWithIndex(
        lambda idx, it: islice(it,1,None) if idx == 0 else it
    )

    # line --> (movieId,(rating,1.0))  - reduce 전에는 1, (4.0 ,1.0)  /   1,(5.0,1.0)  이런식으로 데이터 추출될 것  
    ratings = lines.map(parseInput)  
    
    # reduce 과정 (영화 Id ,(평점 합 , 평점개수의 합)) 윗코드의 추출된 데이터를 1, (9.0, 2.0) 이런식으로 reduce될 것이다. 
    sumAndCount=ratings.reduceByKey(lambda m1,m2: ( m1[0]+m2[0], m1[1]+m2[1]))   # m1 =m1+m2 
    

    # sumAndCount --> (영화 id ,평점의 평균 )
    avgRatings=sumAndCount.mapValues(lambda v: v[0]/ v[1])

    # 평점의 평균 컬럼을 기준으로 sort (오름차순으로)
    sortedMovies=avgRatings.sortBy(lambda x: x[1])  

    # 10개 추출 (현재 정렬된 것은 평점이 낮은순->높은순으로)
    results=sortedMovies.take(10)


# movies에 movieId를 Key로하여 대응되는 movies의 값 Value는 밑 코드에서 movies[result[0]] 이고 title이다. 
# title(제목)과 rating(평점) 컬럼을 가진 row가 10개 추출될것 
    for result in results : 
        print(movies[result[0]],result[1])
