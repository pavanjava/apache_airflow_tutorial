import json


def transform(ti):
    data_list = []
    attributes = ["product_id", "product_name", "quantity_per_unit", "unit_price",
                  "units_in_stock", "units_on_order", "category_id", "category_name",
                  "description"]
    # pull data from the xcom objects
    data = ti.xcom_pull(task_ids=["fetch_data_from_northwind"])[0]
    for row in data:
        data_dict = {}
        for index, data in enumerate(row):
            data_dict[attributes[index]] = str(data)
        data_list.append(data_dict)
    # we can push the data back to xcom so that other operators or tasks can use it.
    # ti.xcom_push(key="products", value=json.dumps(data_list))
    return json.dumps(data_list)
