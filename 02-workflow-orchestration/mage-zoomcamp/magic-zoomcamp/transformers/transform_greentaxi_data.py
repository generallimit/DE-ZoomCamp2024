if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

    data.columns = data.columns.str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True).str.lower()
    print(f'Row count: {data.shape[0]}\nCol count: {data.shape[1]}\n')
    print(data[(data['passenger_count'] > 0) | (data['passenger_count'].isnull()) & (data['trip_distance'] > 0) | (data['trip_distance'].isnull())])

    return data[(data['passenger_count'] > 0) | (data['passenger_count'].isnull()) & (data['trip_distance'] > 0) | (data['trip_distance'].isnull())]
 

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['passenger_count'].isin([0]).sum() == 0 or ['trip_distance'].isin([0]).sum() == 0 or ['passenger_count'].isnotnull() or ['trip_distance'].isnotnull() or ['vendor_id'].isnotnull(), 'There are rides with zero passengers or trip distances of zero'
