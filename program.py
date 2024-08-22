import csv

def parse_flow_log(file_path):
    # Parse the flow log file
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        flow_log_data = [row for row in reader]

    return flow_log_data

def parse_lookup_table(file_path):
    # Parse the lookup table file
    lookup_table = {}
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 3:  # Check if the row has at least 3 columns
                lookup_table[f"{row[0]},{row[1]}"] = row[2]
            # Handle cases with fewer columns if needed, e.g., print a warning
            else:
                print(f"Skipping row with insufficient columns: {row}") # Print a warning for rows with less than 3 columns

    return lookup_table

def map_flow_log_to_tags(flow_log_data, lookup_table):
    # Map each row in the flow log data to a tag based on the lookup table
    tagged_data = []
    for row in flow_log_data:
        if len(row) >= 7: # Check if the row has enough elements
            dst_port = row[5]
            protocol = row[6]
            key = f"{dst_port},{protocol}"
            if key in lookup_table:
                tag = lookup_table[key]
            else:
                tag = "Untagged"
            tagged_data.append((row, tag))
        else:
            print(f"Skipping row with insufficient columns: {row}") # Print a warning for rows with less than 7 columns

    return tagged_data

def generate_output(tagged_data):
    # Generate the output file
    tag_counts = {}
    port_protocol_counts = {}
    for row, tag in tagged_data:
        if tag not in tag_counts:
            tag_counts[tag] = 0
        tag_counts[tag] += 1
        port_protocol_counts[f"{row[5]},{row[6]}"] = port_protocol_counts.get(f"{row[5]},{row[6]}", 0) + 1

    with open("output.txt", 'w') as f:
        f.write("Tag Counts:\n")
        for tag, count in tag_counts.items():
            f.write(f"{tag},{count}\n")
        f.write("\nPort/Protocol Combination Counts:\n")
        for key, count in port_protocol_counts.items():
            f.write(f"{key},{count}\n")

def main():
    flow_log_file_path = "flow_log.txt"
    lookup_table_file_path = "lookup_table.csv"
    flow_log_data = parse_flow_log(flow_log_file_path)
    lookup_table = parse_lookup_table(lookup_table_file_path)
    tagged_data = map_flow_log_to_tags(flow_log_data, lookup_table)
    generate_output(tagged_data)

if __name__ == "__main__":
    main()