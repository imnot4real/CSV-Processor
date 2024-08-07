import argparse
import csv
from collections import defaultdict

def read_csv(file_path):
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        return list(reader)

def write_csv(file_path, data, fieldnames):
    with open(file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def filter_data(data, condition):
    return [row for row in data if eval(condition, None, row)]

def aggregate_data(data, group_by, agg_column, agg_function):
    result = defaultdict(list)
    for row in data:
        key = tuple(row[col] for col in group_by)
        result[key].append(float(row[agg_column]))
    
    agg_result = []
    for key, values in result.items():
        new_row = dict(zip(group_by, key))
        if agg_function == 'sum':
            new_row[f'{agg_function}_{agg_column}'] = sum(values)
        elif agg_function == 'avg':
            new_row[f'{agg_function}_{agg_column}'] = sum(values) / len(values)
        elif agg_function == 'max':
            new_row[f'{agg_function}_{agg_column}'] = max(values)
        elif agg_function == 'min':
            new_row[f'{agg_function}_{agg_column}'] = min(values)
        agg_result.append(new_row)
    
    return agg_result

def transform_data(data, transformations):
    for row in data:
        for new_column, expression in transformations.items():
            row[new_column] = eval(expression, None, row)
    return data

def main():
    parser = argparse.ArgumentParser(description="CSV Processor: Filter, aggregate, and transform CSV data")
    parser.add_argument("input_file", help="Input CSV file path")
    parser.add_argument("output_file", help="Output CSV file path")
    parser.add_argument("--filter", help="Filter condition (e.g., 'int(age) > 30')")
    parser.add_argument("--group-by", nargs="+", help="Columns to group by for aggregation")
    parser.add_argument("--agg-column", help="Column to aggregate")
    parser.add_argument("--agg-function", choices=['sum', 'avg', 'max', 'min'], help="Aggregation function")
    parser.add_argument("--transform", nargs="+", help="Transformations in the format 'new_column=expression'")

    args = parser.parse_args()

    # Read input CSV
    data = read_csv(args.input_file)

    # Apply filter
    if args.filter:
        data = filter_data(data, args.filter)

    # Apply aggregation
    if args.group_by and args.agg_column and args.agg_function:
        data = aggregate_data(data, args.group_by, args.agg_column, args.agg_function)

    # Apply transformations
    if args.transform:
        transformations = dict(t.split('=') for t in args.transform)
        data = transform_data(data, transformations)

    # Write output CSV
    fieldnames = data[0].keys() if data else []
    write_csv(args.output_file, data, fieldnames)

    print(f"Processed data written to {args.output_file}")

if __name__ == "__main__":
    main()