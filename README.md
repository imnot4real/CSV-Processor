# CSV-Processor
Performs operations on CSV files like filtering, aggregating, and transforming data.

ex:
1. Filter rows where age is greater than 30:
   ```
   python csv_processor.py input.csv output.csv --filter "int(age) > 30"
   ```

2. Aggregate data by grouping on 'department' and calculating the average salary:
   ```
   python csv_processor.py input.csv output.csv --group-by department --agg-column salary --agg-function avg
   ```

3. Transform data by adding a new column:
   ```
   python csv_processor.py input.csv output.csv --transform "full_name=f'{first_name} {last_name}'"
   ```

4. Combine multiple operations:
   ```
   python csv_processor.py input.csv output.csv --filter "int(age) > 30" --group-by department --agg-column salary --agg-function avg --transform "average_salary_k=f'{float(avg_salary)/1000:.2f}K'"
   ```
