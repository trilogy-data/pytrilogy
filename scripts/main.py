from click import command, echo


@command()
def main():
    echo("Hello, world!")


if __name__ == "__main__":
    main()
