from trilogy.core.query_processor import generate_cte_name


def test_generate_cte_name():
    names = [f"test_{x}" for x in range(0, 1000)]

    mapped = {}

    for name in names:
        generate_cte_name(name, mapped)

    assert len(mapped) == 1000
