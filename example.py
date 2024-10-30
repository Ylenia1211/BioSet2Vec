from bioset2vec import BioSet2Vec
import time, json, re

def clean_folder_name(folder_name):
    return re.sub(r'^[^a-zA-Z0-9_-]+|[^a-zA-Z0-9_-]+$', '', folder_name.strip())


input_file = "input.json"
params = BioSet2Vec.read_parameters_from_file(input_file)
start_time = time.time()
BioSet2Vec.compute(params)
end_time = time.time()
execution_time_minutes = (end_time - start_time) / 60
n_core = params.get("n_core")
ram = params.get("ram")
input_folder = params.get('folder_path').split(',')
data = {'execution_time_minutes': execution_time_minutes, "n_core": params.get("n_core"), "input_file": input_folder, "ram": str(ram)}

f_name = clean_folder_name(input_folder[0])

with open("execution_time_core_"+ str(n_core) +"_"+ str(f_name) +".json", 'w') as json_file:
    json.dump(data, json_file, indent=4)

print(f"Execution time saved in the file: {execution_time_minutes:.4f} min")
