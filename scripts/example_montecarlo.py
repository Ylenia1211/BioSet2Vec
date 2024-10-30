from bioset2vec import BioSet2Vec
import time, json, re, sys,os
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext




input_file = "input_montecarlo.json" 
params = BioSet2Vec.read_parameters_from_file(input_file)

folder_paths = params.get('folder_path').split(',') # split  quando ho piu set
k_min = params.get('k_min')
k_max = params.get('k_max')
n = params.get('n')
n_core = params.get("n_core")
memory = params.get("ram")
setting_memory = str(memory) + "g"
offHeap_size = params.get("offHeap_size")
path_jar = params.get("path_jar")


if not all(os.path.isdir(folder_path) for folder_path in folder_paths):
    print("Error: One or more specified folder paths are invalid.")
    sys.exit(1)

spark = SparkSession \
            .builder \
            .appName("scala_pyspark") \
            .config("spark.jars", path_jar)\
            .config("spark.driver.cores", str(n_core))\
            .config("spark.driver.memory", setting_memory)\
            .config("spark.executor.cores", str(n_core))\
            .config("spark.rdd.compress", "true")\
            .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
sqlContext = SQLContext(spark)


start_time = time.time()

BioSet2Vec.get_result_montecarlo(spark, input_file)

end_time = time.time()


execution_time_minutes = (end_time - start_time) / 60
print(f"Execution time saved in the file: {execution_time_minutes:.4f} min")

spark.stop()