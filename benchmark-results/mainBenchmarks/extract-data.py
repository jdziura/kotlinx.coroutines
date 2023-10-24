import json
import argparse

parser = argparse.ArgumentParser(description="Extract benchmark data from a JSON file")

parser.add_argument("input_file", help="Path to the input JSON file")
parser.add_argument("output_file", help="Path to the output JSON file")
parser.add_argument("num_cores", type=int, help="Number of CPU cores")

args = parser.parse_args()

with open(args.input_file, 'r') as file:
    data = json.load(file)

extracted_data = []

for entry in data:
    benchmark = entry['benchmark'].split('.')[-1]
    dispatcher = entry['params']['dispatcher']
    score = entry['primaryMetric']['score']
    params = entry['params']
        
    param_string = ','.join(f"{key}={value}" for key, value in params.items() if key != 'dispatcher')
    if param_string:
        benchmark = f"{benchmark}({param_string})"

    extracted_entry = {
        'benchmark': benchmark,
        'dispatcher': dispatcher,
        'score': score,
        'num_cores': args.num_cores
    }
    
    extracted_data.append(extracted_entry)

with open(args.output_file, 'w') as output_file:
    json.dump(extracted_data, output_file, indent=4)

print("Data extraction and saving complete.")
