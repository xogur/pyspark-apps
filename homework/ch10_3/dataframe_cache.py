from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("Company Data Filtering with Cache") \
    .getOrCreate()

# HDFS 경로 설정 (예시)
industries_path = "hdfs:///home/spark/sample/linkedin_jobs/companies/company_industries.csv"
employee_counts_path = "hdfs:///home/spark/sample/linkedin_jobs/companies/employee_counts.csv"

# CSV 로드
industries_df = spark.read.option("header", "true").csv(industries_path)
employee_df = spark.read.option("header", "true").csv(employee_counts_path)

# ✅ 1. 각각 데이터 수 확인
print(f"industries_df count: {industries_df.count()}")
print(f"employee_df count: {employee_df.count()}")

# ✅ 2. employee_df 중복 제거 (company_id 기준)
employee_df_dedup = employee_df.dropDuplicates(["company_id"])

# ✅ 3. employee_count 컬럼을 숫자형으로 변환
employee_df_dedup = employee_df_dedup.withColumn("employee_count", col("employee_count").cast("int"))

# ✅ 4. persist 활용하여 캐싱
employee_df_dedup.count()
industries_df.count()

employee_df_dedup.persist()
industries_df.persist()

# ✅ 5. IT Services and IT Consulting 회사 중 employee_count >= 1000 필터링
filtered_df = industries_df \
    .filter(col("industry") == "ITServicesandITConsulting") \
    .join(employee_df_dedup, on="company_id", how="inner") \
    .filter(col("employee_count") >= 1000) \
    .orderBy(col("employee_count").desc())

# ✅ 6. 결과 출력
filtered_df.select("company_id", "employee_count").show(truncate=False)

# ✅ 7. 잠시 프로그램 유지 (unpersist 없이)
time.sleep(300)

# 세션 종료
spark.stop()
