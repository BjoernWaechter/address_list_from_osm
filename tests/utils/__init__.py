from pyspark import Row


def generic_row_with_schema(data, schema):
    i = 0
    result_data = {}
    for col in schema:
        result_data[col.name] = data[i]
        i += 1

    return Row(**result_data)

