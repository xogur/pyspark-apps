from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col, count
from pyspark.sql.functions import broadcast
import time

# SparkConf 설정
conf = SparkConf()
conf.set("spark.executor.instances", "3")
conf.set("spark.executor.cores", "2")
conf.set("spark.executor.memory", "2g")
conf.set("spark.sql.adaptive.enabled", "false")  # AQE 비활성화

# SparkSession 생성
spark = SparkSession.builder \
    .appName("Skill-wise Job Count with Broadcast Join") \
    .config(conf=conf) \
    .getOrCreate()

# ✅ 파일 경로
job_skills_path = "hdfs:///home/spark/sample/linkedin_jobs/jobs/job_skills.csv"
skills_path = "hdfs:///home/spark/sample/linkedin_jobs/mappings/skills.csv"

# ✅ 파일 로드
job_skills_df = spark.read.option("header", "true").csv(job_skills_path)
skills_df = spark.read.option("header", "true").csv(skills_path)

# ✅ 데이터 타입 캐스팅
job_skills_df = job_skills_df.withColumn("job_id", col("job_id").cast("long"))

# ✅ Broadcast Join 수행
joined_df = job_skills_df.join(
    broadcast(skills_df),
    on="skill_abr",
    how="inner"
)

# ✅ skill_name 기준으로 job 개수 집계
result_df = joined_df.groupBy("skill_name") \
    .agg(count("job_id").alias("job_count")) \
    .orderBy(col("job_count").desc())

# ✅ 결과 확인
result_df.show(truncate=False)

# ✅ count() 출력
print(f"Total distinct skill names: {result_df.count()}")

# ✅ 종료 전 대기
time.sleep(1200)

# 세션 종료
spark.stop()
