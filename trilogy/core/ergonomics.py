from trilogy.constants import CONFIG

# source: https://github.com/aaronbassett/Pass-phrase
CTE_NAMES = """quizzical
highfalutin
wakeful
cheerful
thoughtful
cooperative
questionable
abundant
uneven
yummy
juicy
vacuous
concerned
young
sparkling
abhorrent
sweltering
late
macho
scrawny
friendly
kaput
divergent
busy
charming
protective
premium
puzzled
waggish
rambunctious
puffy
hard
sedate
yellow
resonant
dapper
courageous
vast
cool
elated
wary
bewildered
level
wooden
ceaseless
tearful
cloudy
gullible
flashy
trite
quick
nondescript
round
slow
spiritual
brave
tenuous
abstracted
colossal
sloppy
obsolete
elegant
fabulous
vivacious
exuberant
faithful
helpless
odd
sordid
blue
imported
ugly
ruthless
deeply
eminent
badger
barracuda
bear
boa
cheetah
chimpanzee
civet
cobra
cougar
coyote
crocodile
dingo
eagle
eel
fossa
fox
human
jackal
jaguar
komodo
leopard
lion
lynx
mamba
mandrill
marlin
monitor
ocelot
petrel
python
ray
salamander
serval
shark
skua
tiger
viper
wolf
wolverine
albatross
avocet
budgie
canary
chick
chickadee
chicken
cockatiel
cockatoo
coot
covey
crow
cuckoo
darter
dove
duck
falcon
finch
flamingo
fowl
goldfinch
goose
grouse
hawk
heron
jackdaw
jay
kestrel
lark
loon
macaw
magpie
martin
osprey
ostrich
owl
parakeet
parrot
pelican
penguin
pigeon
pintail
puffin
quail
quetzal
rail
raven
razorbill
rhea
rook
shrike
skylark
snipe
sparrow
starling
stork
swallow
swift
tanager
thrush
toucan
turkey
vulture
warbler""".split(
    "\n"
)


def generate_cte_names():
    if CONFIG.randomize_cte_names:
        from random import shuffle

        new = [*CTE_NAMES]
        shuffle(new)
        return new
    return CTE_NAMES
