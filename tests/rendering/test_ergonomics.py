from trilogy.constants import CONFIG
from trilogy.core.query_processor import generate_cte_name


def test_generate_cte_name():
    names = [f"test_{x}" for x in range(0, 1000)]

    mapped = {}

    for name in names:
        generate_cte_name(name, mapped)

    assert len(mapped) == 1000


def test_randomize_cte_name():
    try:
        CONFIG.randomize_cte_names = False
        names = [f"test_{x}" for x in range(0, 1000)]
        final_names = set()
        for name in names:
            mapped = {}
            name = generate_cte_name(name, mapped)
            final_names.add(name)
        # should be more than 1
        assert final_names == {"quizzical"}
        assert len(list(final_names)) == 1
        CONFIG.randomize_cte_names = True
        names = [f"test_{x}" for x in range(0, 1000)]
        final_names = set()
        for name in names:
            mapped = {}
            name = generate_cte_name(name, mapped)
            final_names.add(name)
        # should be more than 1
        assert len(list(final_names)) > 100
    except Exception as e:
        raise e
    finally:
        CONFIG.randomize_cte_names = False
