import pytest

from trilogy.dialect.results import buffered_rows, streamed_rows

FACTORIES = [buffered_rows, streamed_rows]
COLUMNS = ["n", "label"]
ROWS = [(1, "a"), (2, "b"), (3, "c")]


def counting_rows(rows=ROWS):
    """Yields rows, recording how many have actually been pulled."""
    pulled: list[int] = []

    def source():
        for index, row in enumerate(rows, start=1):
            pulled.append(index)
            yield row

    return source(), pulled


@pytest.mark.parametrize("factory", FACTORIES)
def test_rows_behave_like_sqlalchemy_rows(factory):
    result = factory(COLUMNS, ROWS, "TestRow")

    assert result.keys() == COLUMNS
    row = result.fetchone()
    assert row == (1, "a")
    assert row[1] == "a"
    assert row.label == "a"


@pytest.mark.parametrize("factory", FACTORIES)
def test_reads_consume(factory):
    result = factory(COLUMNS, ROWS, "TestRow")

    assert result.fetchone() == (1, "a")
    assert result.fetchmany(1) == [(2, "b")]
    assert result.fetchall() == [(3, "c")]
    assert result.fetchall() == []
    assert result.fetchone() is None


@pytest.mark.parametrize("factory", FACTORIES)
def test_fetchmany_past_the_end_is_not_an_error(factory):
    result = factory(COLUMNS, ROWS, "TestRow")
    assert result.fetchmany(99) == ROWS


@pytest.mark.parametrize("factory", FACTORIES)
def test_iteration_consumes(factory):
    result = factory(COLUMNS, ROWS, "TestRow")

    assert [tuple(r) for r in result] == ROWS
    assert list(result) == []


@pytest.mark.parametrize("factory", FACTORIES)
def test_no_columns_yields_an_empty_result(factory):
    """DDL/DML jobs report no schema."""
    result = factory([], [])
    assert result.keys() == []
    assert result.fetchall() == []


def test_streamed_rows_pulls_nothing_until_read():
    rows, pulled = counting_rows()
    result = streamed_rows(COLUMNS, rows, "TestRow")

    assert pulled == []
    assert result.fetchone() == (1, "a")
    assert pulled == [1]
    result.fetchmany(1)
    assert pulled == [1, 2]


def test_buffered_rows_reads_everything_up_front():
    rows, pulled = counting_rows()
    buffered_rows(COLUMNS, rows, "TestRow")

    assert pulled == [1, 2, 3]
