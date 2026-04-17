from kindling.data_pipes import DataPipes
from sales_ops.transforms.quality import clean_orders


@DataPipes.pipe(
    pipeid="bronze_to_silver_orders",
    name="Bronze to Silver Orders",
    tags={"layer": "silver"},
    input_entity_ids=["bronze.orders"],
    output_entity_id="silver.orders",
    output_type="table",
)
def bronze_to_silver_orders(bronze_orders):
    return clean_orders(bronze_orders)
