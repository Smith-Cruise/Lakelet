PAIMON_INPUT_FORMAT = "org.apache.paimon.hive.mapred.PaimonInputFormat"
PAIMON_OUTPUT_FORMAT = "org.apache.paimon.hive.mapred.PaimonOutputFormat"
PAIMON_SERDE = "org.apache.paimon.hive.PaimonSerDe"


def create_paimon_table(
    *,
    glue_client,
    database: str,
    table_name: str,
    location: str,
) -> None:
    glue_client.create_table(
        DatabaseName=database,
        TableInput={
            "Name": table_name,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {
                "EXTERNAL": "TRUE",
                "table_type": "PAIMON",
            },
            "StorageDescriptor": {
                "Columns": [],
                "Location": location,
                "InputFormat": PAIMON_INPUT_FORMAT,
                "OutputFormat": PAIMON_OUTPUT_FORMAT,
                "SerdeInfo": {
                    "SerializationLibrary": PAIMON_SERDE,
                    "Parameters": {},
                },
            },
        },
    )
