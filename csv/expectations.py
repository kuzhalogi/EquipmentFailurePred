import great_expectations as gx
context = gx.get_context()
validator = context.sources.pandas_default.read_csv("/home/kuzhalogi/dsp_project/data/onlyfeatures.csv")
validator.expect_column_values_to_not_be_null("pickup_datetime")
validator.expect_column_values_to_be_between(
    "passenger_count", min_value=1, max_value=6
)
validator.save_expectation_suite(discard_failed_expectations=True)
