# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# import time
#
# # Spark 세션 생성
# spark = SparkSession.builder \
#     .appName("Company Data Filtering with Cache") \
#     .getOrCreate()
#
# # HDFS 경로 설정 (예시)
# industries_path = "hdfs:///home/spark/sample/linkedin_jobs/companies/company_industries.csv"
# employee_counts_path = "hdfs:///home/spark/sample/linkedin_jobs/companies/employee_counts.csv"
#
# # CSV 로드
# industries_df = spark.read.option("header", "true").csv(industries_path)
# employee_df = spark.read.option("header", "true").csv(employee_counts_path)
#
# # ✅ 1. 각각 데이터 수 확인
# print(f"industries_df count: {industries_df.count()}")
# print(f"employee_df count: {employee_df.count()}")
#
# # ✅ 2. employee_df 중복 제거 (company_id 기준)
# employee_df_dedup = employee_df.dropDuplicates(["company_id"])
#
# # ✅ 3. employee_count 컬럼을 숫자형으로 변환
# employee_df_dedup = employee_df_dedup.withColumn("employee_count", col("employee_count").cast("int"))
#
# # ✅ 4. persist 활용하여 캐싱
# employee_df_dedup.count()
# industries_df.count()
#
# employee_df_dedup.persist()
# industries_df.persist()
#
# # ✅ 5. IT Services and IT Consulting 회사 중 employee_count >= 1000 필터링
# filtered_df = industries_df \
#     .filter(col("industry") == "ITServicesandITConsulting") \
#     .join(employee_df_dedup, on="company_id", how="inner") \
#     .filter(col("employee_count") >= 1000) \
#     .orderBy(col("employee_count").desc())
#
# # ✅ 6. 결과 출력
# filtered_df.select("company_id", "employee_count").show(truncate=False)
#
# # ✅ 7. 잠시 프로그램 유지 (unpersist 없이)
# time.sleep(300)
#
# # 세션 종료
# spark.stop()

from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import time

spark = SparkSession \
        .builder \
        .appName('dataframe_cache') \
        .getOrCreate()

print(f'spark application start')
company_emp_path = 'hdfs:///home/spark/sample/linkedin_jobs/companies/employee_counts.csv'
company_emp_schema = 'company_id LONG,employee_count LONG,follower_count LONG,time_recorded TIMESTAMP'
company_ind_path = 'hdfs:///home/spark/sample/linkedin_jobs/companies/company_industries.csv'
company_ind_schema = 'company_id LONG, industry STRING'

# employee_counts Load
company_emp_df = spark.read \
                 .option('header','true') \
                 .option('multiLine','true') \
                 .schema(company_emp_schema) \
                 .csv(company_emp_path)
company_emp_df.persist()
emp_cnt = company_emp_df.count()
print(f'company_emp_df count: {emp_cnt}')

# employee_counts 중복 제거
company_emp_dedup_df = company_emp_df.dropDuplicates(['company_id'])
emp_dedup_cnt = company_emp_dedup_df.count()
print(f'company_emp_dedup_df count: {emp_dedup_cnt}')

# company_industries Load
company_idu_df = spark.read \
                 .option('header','true') \
                 .option('multiLine','true') \
                 .schema(company_ind_schema) \
                 .csv(company_ind_path)
company_idu_df.persist()
idu_cnt = company_idu_df.count()
print(f'company_idu_df count: {idu_cnt}')

company_it_df = company_idu_df.filter(col('industry') == 'IT Services and IT Consulting')

company_emp_cnt_df = company_emp_dedup_df.join(
    other=company_it_df,
    on='company_id',
    how='inner'
).select('company_id', 'employee_count') \
    .sort('employee_count',ascending=False)

company_emp_cnt_df.show()
time.sleep(300)
