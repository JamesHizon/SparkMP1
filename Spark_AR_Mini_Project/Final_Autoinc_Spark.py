# Automobile Report Spark Job Python Script

# To run this, make sure you are in the correct working directory and
# type "spark-submit Final_Autoinc_Spark.py" in command line.

from pyspark import SparkConf, SparkContext

# Obtain entry point into Spark
conf = SparkConf().setMaster("local").setAppName("AutoPostSales")
sc = SparkContext(conf=conf)

# Create rdd
raw_rdd = sc.textFile("spark_mp_data.csv")

# Try: Take first four records for unit testing.
# four_row_rdd = raw_rdd.take(4)
# print(type(four_row_rdd))  # Work w/ list object
# print(four_row_rdd)


# Create functions
def extract_vin_key_value(row):
    """
    Map vin_number, make, year and incident_type from each row of csv.

    Next: Debug this function.

    :param row: Row of each csv.
    :return: Dictionary w/ essential data.
    """
    # print(f"row variable: {row}")  # COMMENT OUT FOR TESTING
    row_split = row.split(",")
    # print(f"row_split variable: {row_split}")  # COMMENT OUT FOR TESTING

    vin_number = row_split[2]
    make = row_split[3]
    year = row_split[5]
    incident_type = row_split[1]
    # return {vin_number: [make, year, incident_type]}
    return vin_number, (make, year, incident_type)


# Map function to rdd
vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))


def populate_make(values):
    """
    This function will be applied to a key-value pair.

    Given a list of values, we will first sort the list,
    initialize empty list for output, and then if the value is not empty,
    we will create make and year values to be appended to output list
    with the attached incident_type.

    :param values: Values from key-value pair (applied after groupByKey).
    :return: List of output
    """
    sorted(values)
    # Initialize empty output list which will have make, year and incident_type.
    output_value_list = []

    # Iterate through each of the values
    for val in values:
        # Filter and append to output list if make and year values exist
        if val[0].strip() != '':
            make = val[0]
        if val[1].strip() != '':
            year = val[1]
        # Append values even if there exists no make and year
        output_value_list.append((make, year, val[2]))

    return output_value_list


# Apply unit testing to figure out this issue
enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))


def extract_make_key_value(data):
    """
    Obtain make and count if incident_type == "A". I understand problem wants make and year, but
    for sake of simplicity and understanding, I will just use make.

    We apply this function after iterating through output_value_list of
    prior function.

    :param data: List of output values in format of (make, year, incident_type).
    :return: tuple w/ make, count
    """
    if data[2] == "A":
        return data[0], 1
    else:
        return data[0], 0


# TODO: Debug why .collect() is not working. --> Expected two inputs and only received one.

# Map all values as make_kv
make_kv = enhance_make.map(lambda x: extract_make_key_value(x))  # TODO: Uncomment to see where issue is.

enhance_make = make_kv.reduceByKey(lambda x, y: x + y)  # TODO: Uncomment to see where issue is.
print(enhance_make.collect())

# Collect data --> Issue here. Expects two values but only gets one.
final_rdd = enhance_make.collect()

# Write output to text file
with open("final_output.txt", "w") as fh:
    for item_list in final_rdd:
        print(item_list)
        fh.write(str(item_list) + "\n")

# Stop SparkContext application
sc.stop()
