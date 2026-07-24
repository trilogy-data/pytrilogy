from trilogy.constants import CONFIG

# source: https://github.com/aaronbassett/Pass-phrase
CTE_NAMES = ["quizzical\r", "highfalutin\r", "wakeful\r", "cheerful\r", "thoughtful\r", "cooperative\r", "questionable\r", "abundant\r", "uneven\r", "yummy\r", "juicy\r", "vacuous\r", "concerned\r", "young\r", "sparkling\r", "abhorrent\r", "sweltering\r", "late\r", "macho\r", "scrawny\r", "friendly\r", "kaput\r", "divergent\r", "busy\r", "charming\r", "protective\r", "premium\r", "puzzled\r", "waggish\r", "rambunctious\r", "puffy\r", "hard\r", "sedate\r", "yellow\r", "resonant\r", "dapper\r", "courageous\r", "vast\r", "cool\r", "elated\r", "wary\r", "bewildered\r", "level\r", "wooden\r", "ceaseless\r", "tearful\r", "cloudy\r", "gullible\r", "flashy\r", "trite\r", "quick\r", "nondescript\r", "round\r", "slow\r", "spiritual\r", "brave\r", "tenuous\r", "abstracted\r", "colossal\r", "sloppy\r", "obsolete\r", "elegant\r", "fabulous\r", "vivacious\r", "exuberant\r", "faithful\r", "helpless\r", "odd\r", "sordid\r", "blue\r", "imported\r", "ugly\r", "ruthless\r", "deeply\r", "eminent\r", "badger\r", "barracuda\r", "bear\r", "boa\r", "cheetah\r", "chimpanzee\r", "civet\r", "cobra\r", "cougar\r", "coyote\r", "crocodile\r", "dingo\r", "eagle\r", "eel\r", "fossa\r", "fox\r", "human\r", "jackal\r", "jaguar\r", "komodo\r", "leopard\r", "lion\r", "lynx\r", "mamba\r", "mandrill\r", "marlin\r", "monitor\r", "ocelot\r", "petrel\r", "python\r", "ray\r", "salamander\r", "serval\r", "shark\r", "skua\r", "tiger\r", "viper\r", "wolf\r", "wolverine\r", "albatross\r", "avocet\r", "budgie\r", "canary\r", "chick\r", "chickadee\r", "chicken\r", "cockatiel\r", "cockatoo\r", "coot\r", "covey\r", "crow\r", "cuckoo\r", "darter\r", "dove\r", "duck\r", "falcon\r", "finch\r", "flamingo\r", "fowl\r", "goldfinch\r", "goose\r", "grouse\r", "hawk\r", "heron\r", "jackdaw\r", "jay\r", "kestrel\r", "lark\r", "loon\r", "macaw\r", "magpie\r", "martin\r", "osprey\r", "ostrich\r", "owl\r", "parakeet\r", "parrot\r", "pelican\r", "penguin\r", "pigeon\r", "pintail\r", "puffin\r", "quail\r", "quetzal\r", "rail\r", "raven\r", "razorbill\r", "rhea\r", "rook\r", "shrike\r", "skylark\r", "snipe\r", "sparrow\r", "starling\r", "stork\r", "swallow\r", "swift\r", "tanager\r", "thrush\r", "toucan\r", "turkey\r", "vulture\r", "warbler"]


def generate_cte_names():
    if CONFIG.randomize_cte_names:
        from random import shuffle

        new = [*CTE_NAMES]
        shuffle(new)
        return new
    return CTE_NAMES
