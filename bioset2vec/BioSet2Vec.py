import json, re, os, sys, random
import pyspark.sql.types as T
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
from pyspark import RDD
from pyspark.mllib.common import _py2java, _java2py
import pandas as pd
import pyarrow.dataset as ds
import numpy as np
from pyspark.sql.functions import col, lit, expr, length
from pyspark.sql.functions import sumDistinct, col, sum 
from tqdm import tqdm
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType  
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import explode, col
from pyspark.sql.functions import max as sp_max, when


# Read input parameters from JSON file
def read_parameters_from_file(file_path):
    with open(file_path, 'r') as file:
        parameters = json.load(file)
    return parameters


def rename_file(original_name):
    new_name = re.sub(r'out_N\d+', '', original_name)
    new_name = new_name.replace('__', '_')
    if new_name.endswith('_'):
        new_name = new_name[:-1]
    return new_name



def compute_simple_tf(folder_path, sc, sqlContext, files_fasta, k_min, k_max):
    files_fasta_out = files_fasta.split("/")[-1]
    tempdir = "./"
    for k in tqdm(range(k_min, k_max+1)):
            numrow = sc._jvm.bioft.TFApp.process(k, tempdir, files_fasta,  sc._jsc, sqlContext._jsqlContext)
            df_spark = _java2py(sc, numrow)
            id_file = files_fasta.split("/")[-1]
            
            original_name = id_file
            new_name = rename_file(original_name)

            id_file = new_name
            size_doc = df_spark.select("_2").agg(sum("_2")).collect()[0][0]
            result_df = df_spark.withColumnRenamed("_2", "count")\
                                .withColumnRenamed("_1", "kmers")\
         
            new_folder_path =  "./" + folder_path.split("/")[1] + "/res/" + folder_path.split("/")[2]
            #print("F:", new_folder_path)
            
            result_df_replaced_iupac = replace_with_iupac(new_folder_path, result_df) 
            result_df_replaced_iupac = result_df_replaced_iupac.drop("kmers_no_replicated")

            result_df_replaced_iupac = result_df_replaced_iupac.groupBy("kmers").agg(sum("count").alias("count"))
            
            result_df_replaced_iupac = result_df_replaced_iupac.withColumn("kmers_doc", lit(size_doc))\
                                 .withColumn("TF", expr("count/kmers_doc"))\
                                 .withColumn("id_file", lit(id_file))\
                                .withColumn("count", col("count").cast(IntegerType()))

            result_df_replaced_iupac.write.mode("overwrite").parquet(new_folder_path +"resultsTF/"+ "tf_"+ str(k) +"/" +files_fasta_out)


                
def create_input_fasta(files):
    all_sequences_set = []
    for file_path in files:
        all_sequences = []
        with open(file_path, 'r') as file:
            data = file.read()
            all_sequences.append(data)
    all_sequences_set.append(all_sequences)
    # Join all the resulting strings together
    return ''.join([''.join(sublist) for sublist in all_sequences_set])

        
def save_file_fasta(folder_path, name_file,  result_string, i):
    output_file = folder_path + name_file + "out_N" + str(i) + ".fa" #tmp file
    with open(output_file, "w") as file:
        file.write(result_string)
    return output_file 

def extract_sequences(data):
    sequences = []
    lines = data.split('\n')
    current_sequence = ""
    for line in lines:
        if line.startswith(">"):
            if current_sequence:
                sequences.append(current_sequence)
                current_sequence = ""
        else:
            current_sequence += line.strip()
    if current_sequence:
        sequences.append(current_sequence)
    return sequences  

            
#For Monte Carlo computations generate_synthetic_sequence 
def generate_synthetic_sequence(data, k_min, k_max, n, seq_length):
    # For Monte Carlo computations
    # Determine sequence length from file length
    #print("seq_length= ", seq_length)
    for _ in range(n):
        # Generate a new seed for each Monte Carlo test
        seed = random.randint(0, sys.maxsize)
        random.seed(seed)
        # Generate a random DNA sequence of specified length
        dna_sequence = ''.join(random.choices('ATGC', k=seq_length))
        #print(dna_sequence, "\n", len(dna_sequence))
    return dna_sequence


def process_files(files, k_min, k_max, n):
    all_sequences_set = []
    for file_path in files:
        all_sequences = []
        with open(file_path, 'r') as file:
            data = file.read()
            sequences = extract_sequences(data)
            # Perform operations on each sequence
            for sequence in sequences:
               # Perform operations on the single seq data
                seq_length = len(sequence)
                processed_data = generate_synthetic_sequence(sequence, k_min, k_max, n, seq_length)
                processed_data= ">sintetic seq "+file_path + " len " + str(len(processed_data))  + "\n" + processed_data + "\n"
                all_sequences.append(processed_data)
                
            
            all_sequences_set.append(all_sequences)
    return all_sequences_set
    
def process_files_in_folders(sc,sqlContext, folder_paths, k_min, k_max, n):
    for folder_path in folder_paths: 
        files = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, file))]
        files = [file for file in files if not file.endswith('.DS_Store')]
        #print(files)
        for file_fasta in files:
            file = [str(file_fasta)]
            if n>=1 and len(file)>=1:
                for i in range(1, n+1):
                    all_sequences = process_files(file, int(k_min), int(k_max), int(n))
                    #Join each sublist's elements into a string
                    substrings = [''.join(sublist) for sublist in all_sequences]
                    # Join all the resulting strings together
                    result_string = ''.join(substrings)
                    name_file = file[0].split("/")[-1].split(".")[0]
                    output_file = save_file_fasta(folder_path, name_file,  result_string, i)
                    #print("OUTP ", output_file)
                    compute_simple_tf(folder_path+"N"+str(i), sc,sqlContext, output_file, k_min, k_max)
                    if os.path.exists(output_file):
                        os.remove(output_file)
                        #print(f"File {output_file} removed.")
            else: 
                #print("no montecarlo")
                compute_simple_tf(folder_path, sc,sqlContext, file_fasta, k_min, k_max)
                #name_file = file[0].split("/")[-1].split(".")[0]
                #file_fasta = create_input_fasta(file)
                #output_file = save_file_fasta(folder_path, name_file,  file_fasta, n)
                #compute_simple_tf(folder_path, sc,sqlContext, output_file, k_min, k_max)
                #if os.path.exists(output_file):
                #    os.remove(output_file)
                #    print(f"File {output_file} removed.")
                

def get_tf_all_k_single_doc(df, id_file): 
    #TF on all k
    filter_df_single_doc = df.where(col("id_file") == id_file)  
    size_doc_union = filter_df_single_doc.where(col("id_file") == id_file).select("kmers_doc").distinct().agg(sum("kmers_doc")).collect()[0][0]
    result_df_union = filter_df_single_doc.withColumn("kmers_doc_union", lit(size_doc_union))\
                                          .withColumnRenamed("TF", "TF_old")\
                                          .withColumn("TF", expr("count/kmers_doc_union"))
    return result_df_union




def generate_kmers(kmer):
    iupac_mapping = {
        'r': ['a', 'g'],  # puRine
        'y': ['c', 't'],  # pYrimidine
        'k': ['g', 't'],  # Keto
        'm': ['a', 'c'],  # aMino
        's': ['g', 'c'],  # Strong base pair ['G', 'C']
        'w': ['a', 't'],  # Weak base pair
        'n': ['a', 'c', 'g', 't'],  # Any base
        'b': ['c', 'g', 't'],  # not A
        'd': ['a', 'g', 't'],  # not C
        'h': ['a', 'c', 't'],  # not G
        'v': ['a', 'c', 'g']   # not T/U
    }
    
    kmer_split = list(kmer)
    possible_kmers = []

    # Generate combinations
    def generate_combinations(pos, current_kmer):
        if pos == len(kmer_split):
            possible_kmers.append(''.join(current_kmer))
            return

        current_char = kmer_split[pos]
        if current_char in iupac_mapping:
            for replacement in iupac_mapping[current_char]:
                current_kmer[pos] = replacement
                generate_combinations(pos + 1, current_kmer)
        else:
            current_kmer[pos] = current_char
            generate_combinations(pos + 1, current_kmer)

    generate_combinations(0, [None] * len(kmer_split))
    return possible_kmers



def compute_tf_k_k(spark, path,k_min, k_max):

    schema = StructType([
        StructField("kmers", StringType(), True),    
        StructField("count", IntegerType(), True),
        StructField("id_file", StringType(), True), 
        StructField("kmers_doc", IntegerType(), True),
        StructField("TF", DoubleType(), True),
    ])

    first_parquet_file =spark.read.schema(schema).load(path+str(k_min) +"/*")

    for k in range(k_min+1,k_max+1):
        folder_path_montecarlo = path + str(k) +"/*"
        tmp_tf_tmp =spark.read.schema(schema).load(folder_path_montecarlo)
        first_parquet_file = first_parquet_file.union(tmp_tf_tmp)


    distinct_id_files = first_parquet_file.select("id_file").distinct().collect()
    distinct_id_list = [row.id_file for row in distinct_id_files]
    result_df_union_all_docs = get_tf_all_k_single_doc(first_parquet_file, distinct_id_list[0])

    for id_list in range(1, len(distinct_id_list)):
        result_df_union_tmp = get_tf_all_k_single_doc(first_parquet_file, distinct_id_list[id_list])
        result_df_union_all_docs = result_df_union_all_docs.union(result_df_union_tmp)


    new_folder_path_tf = path + str(k_min) + "_" + str(k_max) 
    result_df_union_all_docs.write.mode("overwrite").parquet(new_folder_path_tf)

    return new_folder_path_tf


def replace_with_iupac(new_folder_path_tf, result_df_union_all_docs):
    generate_kmers_udf = udf(generate_kmers, ArrayType(StringType()))
    df_replicated_iupac = result_df_union_all_docs.withColumnRenamed("kmers", "kmers_no_replicated").withColumn("kmers", explode(generate_kmers_udf(col("kmers_no_replicated"))))
    return df_replicated_iupac

    
def get_result_montecarlo(spark, input_file):
    
    params = read_parameters_from_file(input_file)
    print(params)
    
    folder_paths = params.get('folder_path').split(',') # split  quando ho piu set
    #folder_path = params.get('folder_path')
    k_min = params.get('k_min')
    k_max = params.get('k_max')
    n = params.get('n')
    
    folder_result_real = folder_paths[0] + "res" + "/resultsTFIDF/tf_idf"
    df_tfidf = spark.read.parquet(folder_result_real)
    result_list = [row["id_file"] for row in df_tfidf.select("id_file").distinct().collect()]

    for doc in result_list:
        #print(doc)
        #df_res_single_set = df_tfidf.where(col("id_file") == "file1.fa")
        df_res_single_set = df_tfidf.where(col("id_file") == doc)
        df_union_montecarlo =  spark.createDataFrame([], df_res_single_set.schema)
        for i in range(1, n+1): 
            folder_result_montecarlo =  folder_paths[0] + "res/"+ "N" + str(i)  + "resultsTFIDF/tf_idf"
            df_tfidf_montecarlo = spark.read.parquet(folder_result_montecarlo)
            df_union_montecarlo = df_union_montecarlo.union(df_tfidf_montecarlo)

        list_of_kmers_SET = df_res_single_set.select("kmers").rdd.flatMap(lambda x: x).collect()
        df_montecarlo = df_union_montecarlo.filter(col("kmers").isin(list_of_kmers_SET))

        df_montecarlo_max_v = df_montecarlo.groupBy("kmers").agg(sp_max("TF-IDF").alias("max_TF-IDF")) #.alias("max_TF-IDF")

        df_res_single_set = df_res_single_set.withColumnRenamed("TF-IDF", "TF-IDF_real")

        df1_merge_real_fake = df_montecarlo_max_v.join(df_res_single_set, on="kmers", how="outer")
        df1_merge_real_fake = df1_merge_real_fake.fillna({'max_TF-IDF': 0}) 
        df1_merge_real_fake = df1_merge_real_fake.withColumn(
            "is_score_valid",
            when(col("TF-IDF_real") >= col("max_TF-IDF"), "yes").otherwise("no")
        )

        df1_merge_real_fake = df1_merge_real_fake.sort(col("TF-IDF_real").desc())

        folder_res_montecarlo_set= folder_paths[0] + "res/montecarlo_score/result_"+ doc
        df1_merge_real_fake.write.mode("overwrite").parquet(folder_res_montecarlo_set)


def should_exclude(item_name):
    return item_name in ['.ipynb_checkpoints', '.DS_Store', 'tfidf', 'tf_10']


def compute_tfidf(sc, sqlContext, spark, pathTF,pathTFIDF):
    return sc._jvm.bioft.TFIDFApp.compute_tfidf(pathTF,pathTFIDF, sc._jsc, sqlContext._jsqlContext, spark._jsparkSession)


def compute(params):

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
                #.config("spark.memory.offHeap.enabled", "true")\
                #.config("spark.memory.offHeap.size", offHeap_size)\ 
                #.config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")\
                #.config("spark.cleaner.referenceTracking.blocking.shuffle", "true")\
                

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    #print("Spark UI running on:", spark.conf.get("spark.ui.port"))

    sqlContext = SQLContext(spark)
    
    process_files_in_folders(sc, sqlContext, folder_paths, k_min, k_max, n)
    
    
    
    if len(folder_paths)>=2:
        #TODO: SU PIU PATH  
        print("TODO")
        """
        for folder in folder_paths:
            print(folder)
            if (n>=1):
                for i in range(1, n+1):
                    for k in range(k_min,k_max+1):
                        folder_path_montecarlo =  "./" + folder.split("/")[1] + "/res/" + folder.split("/")[2] + "N" + str(i) + "resultsTF/tf_"+str(k) +"/*"
                        print(folder_path_montecarlo)
                        df_tf_tmp =spark.read.schema(schema).load(folder_path_montecarlo)
                        df_tf_tmp.show()
            else: 
                for k in range(k_min,k_max+1):
                    folder_path_no_montecarlo =  "./" + folder.split("/")[1] + "/res/" + folder.split("/")[2]+ "resultsTF/tf_"+str(k) +"/*"
                    print(folder_path_no_montecarlo)
                    df_tf_tmp =spark.read.schema(schema).load(folder_path_no_montecarlo)
                    df_tf_tmp.show()
        """
    else:
        if (n>=1):
            for i in range(1, n+1):
                path =  folder_paths[0] + "res/"+ "N" + str(i) + "resultsTF/tf_"
                pathTF = compute_tf_k_k(spark, path,k_min, k_max) #new_folder_path_tf
                pathTFIDF =  folder_paths[0] + "res/"+ "N" + str(i)  + "resultsTFIDF/" 
                tfidf = sc._jvm.bioft.TFIDFApp.compute_tfidf(pathTF,pathTFIDF, sc._jsc, sqlContext._jsqlContext, spark._jsparkSession)
        else:
            path = folder_paths[0]+ "/res/resultsTF/tf_"
            pathTF = compute_tf_k_k(spark, path,k_min, k_max) #new_folder_path_tf
            pathTFIDF =  folder_paths[0] + "res/" + "resultsTFIDF/"
            tfidf = sc._jvm.bioft.TFIDFApp.compute_tfidf(pathTF,pathTFIDF, sc._jsc, sqlContext._jsqlContext, spark._jsparkSession)


    spark.stop()