from preql import Environment


def test_multi_environment():
    basic = Environment()

    basic.parse(
        """
const pi <- 3.14;
                     

""",
        namespace="math",
    )

    basic.parse(
        """
                
            select math.pi;
                """
    )

    assert basic.concepts["math.pi"].name == "pi"
