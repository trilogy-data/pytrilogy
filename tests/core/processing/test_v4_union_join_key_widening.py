"""Join-key widening over a UnionNode: a union renders what EVERY arm renders,
is widened arm by arm, and never descends into its arms one at a time."""

from trilogy import Environment
from trilogy.core.processing.nodes import SelectNode, UnionNode
from trilogy.core.processing.v4_helper.projection import (
    renderable_addresses,
    widen_projection,
)
from trilogy.core.processing.v4_helper.strategy_builder import _widen_scan_chain

MODEL = """
key id string;
key part enum<string>['x', 'y'];
property id.lat float;
property id.lon float;
property id.note string;

root partial datasource px (id: id, part: part, lat: lat, lon: lon, note: note)
grain (id)
complete where part = 'x'
query '''select 'a' as id, 'x' as part, 1.0 as lat, 1.0 as lon, 'n' as note''';

root partial datasource py (id: id, part: part, lat: lat, lon: lon)
grain (id)
complete where part = 'y'
query '''select 'b' as id, 'y' as part, 2.0 as lat, 2.0 as lon''';

auto cell <- concat(cast(floor(lat) as string), ':', cast(floor(lon) as string));
auto tagged <- concat(id, note);
"""


def _union():
    env = Environment()
    env.parse(MODEL)
    build = env.materialize_for_select()
    projected = [build.concepts["id"], build.concepts["part"]]
    arms = [
        SelectNode(
            input_concepts=projected,
            output_concepts=projected,
            environment=build,
            datasource=build.datasources[name],
        )
        for name in ("px", "py")
    ]
    union = UnionNode(
        input_concepts=projected,
        output_concepts=projected,
        environment=build,
        parents=arms,
    )
    return build, union


def test_union_renders_what_every_arm_binds():
    _, union = _union()
    available = renderable_addresses(union)
    assert {"local.lat", "local.lon"} <= available
    assert "local.note" not in available


def test_scan_chain_widens_union_and_every_arm():
    build, union = _union()
    cell = build.concepts["cell"]
    assert _widen_scan_chain(union, cell)
    assert cell.address in {o.address for o in union.output_concepts}
    for arm in union.parents:
        assert cell.address in {o.address for o in arm.output_concepts}
    assert "local.lat" not in {i.address for i in union.input_concepts}


def test_scan_chain_declines_a_column_one_arm_cannot_render():
    build, union = _union()
    tagged = build.concepts["tagged"]
    assert not _widen_scan_chain(union, tagged)
    assert not widen_projection(union, [tagged])
    assert tagged.address not in {o.address for o in union.output_concepts}
    for arm in union.parents:
        assert tagged.address not in {o.address for o in arm.output_concepts}
