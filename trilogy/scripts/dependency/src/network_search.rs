//! The v4 source-network enumeration walk (`_enumerate_covers`), ported from
//! `trilogy/core/processing/v4_helper/network_search.py` + `network_obligations.py`
//! + the walk-facing slice of `network_model.py`/`network_topology.py`.
//!
//! The Python implementation is the spec. Semantics that must survive exactly
//! (docs/handoff_rust_network_search.md): level-order walk with deterministic
//! push order, proper-superset dominance against emitted covers by binding
//! profile, scarcest-obligation branching with `(len, identity)` tiebreak,
//! visited dedup by state set, both budgets reported by name, soft full-binder
//! branches after each emit.
//!
//! Everything is interned up front: node ids and address ids are assigned in
//! sorted-name order, so integer comparison reproduces Python's string
//! ordering everywhere ordering is load-bearing (UTF-8 byte order equals code
//! point order, which is Python's `str` ordering).

use std::collections::HashMap;
use std::collections::HashSet;

/// Fixed-width bitset. All sets of one universe (nodes, addresses, terminal
/// positions) share a width, so ops never reallocate.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Bits {
    words: Vec<u64>,
}

impl Bits {
    fn new(universe: usize) -> Self {
        Bits {
            words: vec![0; universe.div_ceil(64)],
        }
    }

    fn insert(&mut self, index: usize) {
        self.words[index / 64] |= 1u64 << (index % 64);
    }

    fn contains(&self, index: usize) -> bool {
        (self.words[index / 64] >> (index % 64)) & 1 == 1
    }

    fn with(&self, index: usize) -> Bits {
        let mut out = self.clone();
        out.insert(index);
        out
    }

    fn intersects(&self, other: &Bits) -> bool {
        self.words.iter().zip(&other.words).any(|(a, b)| a & b != 0)
    }

    fn is_subset(&self, other: &Bits) -> bool {
        self.words.iter().zip(&other.words).all(|(a, b)| a & !b == 0)
    }

    fn is_proper_subset(&self, other: &Bits) -> bool {
        self.is_subset(other) && self != other
    }

    fn count(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    fn is_empty(&self) -> bool {
        self.words.iter().all(|w| *w == 0)
    }

    /// Ascending index order — id order is sorted-name order, so this is
    /// exactly Python's `sorted(...)` iteration.
    fn ones(&self) -> impl Iterator<Item = usize> + '_ {
        self.words.iter().enumerate().flat_map(|(wi, w)| {
            let mut word = *w;
            std::iter::from_fn(move || {
                if word == 0 {
                    return None;
                }
                let bit = word.trailing_zeros() as usize;
                word &= word - 1;
                Some(wi * 64 + bit)
            })
        })
    }
}

pub struct CandidateSpec {
    pub node: String,
    /// (address, partial)
    pub bindings: Vec<(String, bool)>,
    pub grain: Vec<String>,
    /// `ConditionFit.partial_is_full` (IMPLIED_EXACT).
    pub partial_is_full: bool,
}

pub struct NetworkSpec {
    pub terminals: Vec<String>,
    pub candidates: Vec<CandidateSpec>,
    /// (class representative, member carrier lists — order is meaningful).
    pub axis_families: Vec<(String, Vec<Vec<String>>)>,
    /// (canonical, left side keys, right side keys).
    pub join_requirements: Vec<(String, Vec<String>, Vec<String>)>,
    /// arm node -> subsuming union node.
    pub subsumed_arms: Vec<(String, String)>,
    pub cover_limit: usize,
    pub state_limit: usize,
}

#[derive(PartialEq, Eq, Debug)]
pub enum LimitKind {
    Covers,
    States,
}

/// `ObligationKind`, ranked by the STRING order of the Python enum values —
/// the `identity` tiebreak compares those strings.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    Axis = 0,      // "axis"
    Colocated = 1, // "colocated"
    Connected = 2, // "connected"
    Cover = 3,     // "cover"
    Labelable = 4, // "labelable"
    Paired = 5,    // "paired"
}

/// A pending obligation reduced to what the branch choice reads: the
/// `(len(satisfiers), identity)` key and the satisfiers themselves.
struct Ob {
    kind: Kind,
    subject: Vec<u32>,
    satisfiers: Vec<u32>,
}

impl Ob {
    fn key(&self) -> (usize, Kind, &[u32]) {
        (self.satisfiers.len(), self.kind, &self.subject)
    }
}

struct AxisMember {
    nodes: Vec<u32>,
    bits: Bits,
    /// Rank of `str(index)` under string ordering within this family — the
    /// subject's second element compares as Python's stringified index does.
    subject_rank: u32,
}

struct AxisFamily {
    representative: u32,
    members: Vec<AxisMember>,
}

struct Side {
    subject: Vec<u32>,
    carrier_bits: Bits,
    materializer_bits: Bits,
    satisfiers: Vec<u32>,
}

struct Net {
    n_nodes: usize,
    node_names: Vec<String>,
    terminals: Vec<u32>,
    binding_keys: Vec<Bits>,
    grain: Vec<Bits>,
    grain_nonempty: Vec<bool>,
    row_complete: Vec<bool>,
    /// Per terminal position, over nodes.
    binder_bits: Vec<Bits>,
    binders: Vec<Vec<u32>>,
    full_binder_bits: Vec<Bits>,
    full_binders: Vec<Vec<u32>>,
    /// Per node, over terminal positions.
    bound_terminals: Vec<Bits>,
    join_partners: Vec<Bits>,
    functional_partners: Vec<Bits>,
    func_succ: Vec<Bits>,
    func_pred: Vec<Bits>,
    /// Per terminal position, over nodes.
    chain_completers: Vec<Bits>,
    axis_families: Vec<AxisFamily>,
    axis_family_index: HashMap<u32, usize>,
    sides: Vec<Side>,
    /// Per node: the union node subsuming this partition arm, if any.
    subsumed: Vec<Option<u32>>,
    /// COVER satisfiers per terminal position, already arm-pruned.
    cover_satisfiers: Vec<Vec<u32>>,
}

fn intern(sorted: &[String], name: &str) -> u32 {
    sorted
        .binary_search_by(|probe| probe.as_str().cmp(name))
        .unwrap_or_else(|_| panic!("uninterned name: {name}")) as u32
}

impl Net {
    fn build(spec: &NetworkSpec) -> Net {
        let mut node_names: Vec<String> =
            spec.candidates.iter().map(|c| c.node.clone()).collect();
        node_names.sort();
        node_names.dedup();
        let n_nodes = node_names.len();

        let mut addr_names: Vec<String> = Vec::new();
        addr_names.extend(spec.terminals.iter().cloned());
        for candidate in &spec.candidates {
            addr_names.extend(candidate.bindings.iter().map(|(a, _)| a.clone()));
            addr_names.extend(candidate.grain.iter().cloned());
        }
        for (representative, _) in &spec.axis_families {
            addr_names.push(representative.clone());
        }
        for (canonical, left, right) in &spec.join_requirements {
            addr_names.push(canonical.clone());
            addr_names.extend(left.iter().cloned());
            addr_names.extend(right.iter().cloned());
        }
        addr_names.sort();
        addr_names.dedup();
        let n_addrs = addr_names.len();

        let node_id = |name: &str| intern(&node_names, name) as usize;
        let addr_id = |name: &str| intern(&addr_names, name) as usize;

        let mut binding_keys = vec![Bits::new(n_addrs); n_nodes];
        let mut partial_binds = vec![Bits::new(n_addrs); n_nodes];
        let mut grain = vec![Bits::new(n_addrs); n_nodes];
        let mut partial_is_full = vec![false; n_nodes];
        for candidate in &spec.candidates {
            let node = node_id(&candidate.node);
            partial_is_full[node] = candidate.partial_is_full;
            for (address, partial) in &candidate.bindings {
                let addr = addr_id(address);
                binding_keys[node].insert(addr);
                if *partial {
                    partial_binds[node].insert(addr);
                }
            }
            for address in &candidate.grain {
                grain[node].insert(addr_id(address));
            }
        }
        let grain_nonempty: Vec<bool> = grain.iter().map(|g| !g.is_empty()).collect();
        // `_row_complete`: IMPLIED_EXACT, or every grain-address binding FULL.
        let row_complete: Vec<bool> = (0..n_nodes)
            .map(|node| partial_is_full[node] || !grain[node].intersects(&partial_binds[node]))
            .collect();
        // `binds_fully`: bound, and not partial or IMPLIED_EXACT.
        let full_binds: Vec<Bits> = (0..n_nodes)
            .map(|node| {
                if partial_is_full[node] {
                    binding_keys[node].clone()
                } else {
                    let mut bits = binding_keys[node].clone();
                    for (word, partial) in bits.words.iter_mut().zip(&partial_binds[node].words) {
                        *word &= !partial;
                    }
                    bits
                }
            })
            .collect();

        let terminals: Vec<u32> = spec.terminals.iter().map(|t| addr_id(t) as u32).collect();
        let n_terms = terminals.len();

        let mut binder_bits = vec![Bits::new(n_nodes); n_terms];
        let mut full_binder_bits = vec![Bits::new(n_nodes); n_terms];
        for (position, terminal) in terminals.iter().enumerate() {
            for node in 0..n_nodes {
                if binding_keys[node].contains(*terminal as usize) {
                    binder_bits[position].insert(node);
                }
                if full_binds[node].contains(*terminal as usize) {
                    full_binder_bits[position].insert(node);
                }
            }
        }
        let binders: Vec<Vec<u32>> = binder_bits
            .iter()
            .map(|bits| bits.ones().map(|n| n as u32).collect())
            .collect();
        let full_binders: Vec<Vec<u32>> = full_binder_bits
            .iter()
            .map(|bits| bits.ones().map(|n| n as u32).collect())
            .collect();
        let mut bound_terminals = vec![Bits::new(n_terms); n_nodes];
        for (position, bits) in binder_bits.iter().enumerate() {
            for node in bits.ones() {
                bound_terminals[node].insert(position);
            }
        }

        // Pairwise predicates, each unordered pair asked once (`_partners`).
        let mut join_partners = vec![Bits::new(n_nodes); n_nodes];
        let mut functional_partners = vec![Bits::new(n_nodes); n_nodes];
        let mut func_succ = vec![Bits::new(n_nodes); n_nodes];
        let mut func_pred = vec![Bits::new(n_nodes); n_nodes];
        let mut shared = Bits::new(n_addrs);
        for left in 0..n_nodes {
            for right in (left + 1)..n_nodes {
                for (index, word) in shared.words.iter_mut().enumerate() {
                    *word = binding_keys[left].words[index] & binding_keys[right].words[index];
                }
                if shared.is_empty() {
                    continue;
                }
                join_partners[left].insert(right);
                join_partners[right].insert(left);
                // `joins_functionally`: either grain covered by the shared
                // keys — an EMPTY grain counts as covered, matching Python's
                // `frozenset() <= keys`.
                if grain[left].is_subset(&shared) || grain[right].is_subset(&shared) {
                    functional_partners[left].insert(right);
                    functional_partners[right].insert(left);
                }
                // `functional_into` both directions: target grain non-empty
                // and covered by the shared keys.
                if grain_nonempty[right] && grain[right].is_subset(&shared) {
                    func_succ[left].insert(right);
                    func_pred[right].insert(left);
                }
                if grain_nonempty[left] && grain[left].is_subset(&shared) {
                    func_succ[right].insert(left);
                    func_pred[left].insert(right);
                }
            }
        }

        // `chain_completers`: ancestors of the full binders, expanded only
        // through row-complete nodes.
        let chain_completers: Vec<Bits> = (0..n_terms)
            .map(|position| {
                let mut seen = full_binder_bits[position].clone();
                let mut stack: Vec<usize> = seen.ones().collect();
                while let Some(current) = stack.pop() {
                    for origin in func_pred[current].ones() {
                        if seen.contains(origin) {
                            continue;
                        }
                        seen.insert(origin);
                        if row_complete[origin] {
                            stack.push(origin);
                        }
                    }
                }
                seen
            })
            .collect();

        let mut subsumed: Vec<Option<u32>> = vec![None; n_nodes];
        for (arm, union_node) in &spec.subsumed_arms {
            subsumed[node_id(arm)] = Some(node_id(union_node) as u32);
        }
        let prune = |satisfiers: Vec<u32>| prune_arms(&subsumed, satisfiers);

        // `sorted(network.axis_families.items())` — keys are unique, so the
        // representative alone orders the families.
        let mut axis_families: Vec<AxisFamily> = spec
            .axis_families
            .iter()
            .map(|(representative, members)| {
                let mut index_strings: Vec<String> =
                    (0..members.len()).map(|i| i.to_string()).collect();
                index_strings.sort();
                AxisFamily {
                    representative: addr_id(representative) as u32,
                    members: members
                        .iter()
                        .enumerate()
                        .map(|(index, nodes)| {
                            let ids: Vec<u32> =
                                nodes.iter().map(|n| node_id(n) as u32).collect();
                            let mut bits = Bits::new(n_nodes);
                            for id in &ids {
                                bits.insert(*id as usize);
                            }
                            AxisMember {
                                nodes: ids,
                                bits,
                                subject_rank: index_strings
                                    .binary_search(&index.to_string())
                                    .unwrap() as u32,
                            }
                        })
                        .collect(),
                }
            })
            .collect();
        axis_families.sort_by_key(|family| family.representative);
        let axis_family_index: HashMap<u32, usize> = axis_families
            .iter()
            .enumerate()
            .map(|(index, family)| (family.representative, index))
            .collect();

        // PAIRED sides: carriers, materializers and satisfiers are all
        // cover-independent, so they are computed once.
        let mut sides: Vec<Side> = Vec::new();
        for (canonical, left, right) in &spec.join_requirements {
            let canonical_addr = addr_id(canonical);
            for keys in [left, right] {
                if keys.is_empty() {
                    continue;
                }
                let mut key_ids: Vec<u32> = keys.iter().map(|k| addr_id(k) as u32).collect();
                key_ids.sort();
                key_ids.dedup();
                let mut key_bits = Bits::new(n_addrs);
                for key in &key_ids {
                    key_bits.insert(*key as usize);
                }
                let mut carrier_bits = Bits::new(n_nodes);
                let mut materializer_bits = Bits::new(n_nodes);
                let mut materializers: Vec<u32> = Vec::new();
                for node in 0..n_nodes {
                    if !key_bits.is_subset(&binding_keys[node]) {
                        continue;
                    }
                    carrier_bits.insert(node);
                    if binding_keys[node].contains(canonical_addr) {
                        materializer_bits.insert(node);
                        materializers.push(node as u32);
                    }
                }
                // `(not grain <= keys, node)`: the dimension these keys
                // identify before any wider scan carrying both.
                materializers.sort_by_key(|node| {
                    (!grain[*node as usize].is_subset(&key_bits), *node)
                });
                let mut subject = vec![canonical_addr as u32];
                subject.extend(key_ids.iter());
                sides.push(Side {
                    subject,
                    carrier_bits,
                    materializer_bits,
                    satisfiers: prune(materializers),
                });
            }
        }

        let cover_satisfiers: Vec<Vec<u32>> =
            binders.iter().map(|list| prune(list.clone())).collect();

        Net {
            n_nodes,
            node_names,
            terminals,
            binding_keys,
            grain,
            grain_nonempty,
            row_complete,
            binder_bits,
            binders,
            full_binder_bits,
            full_binders,
            bound_terminals,
            join_partners,
            functional_partners,
            func_succ,
            func_pred,
            chain_completers,
            axis_families,
            axis_family_index,
            sides,
            subsumed,
            cover_satisfiers,
        }
    }

    /// `_binding_profile`: per-terminal bound level, axis-aware.
    fn profile(&self, chosen: &Bits) -> Vec<u8> {
        self.terminals
            .iter()
            .enumerate()
            .map(|(position, terminal)| {
                if let Some(family_index) = self.axis_family_index.get(terminal) {
                    if self.axis_families[*family_index]
                        .members
                        .iter()
                        .all(|member| member.bits.intersects(chosen))
                    {
                        return 2;
                    }
                } else if self.full_binder_bits[position].intersects(chosen) {
                    return 2;
                }
                if self.binder_bits[position].intersects(chosen) {
                    1
                } else {
                    0
                }
            })
            .collect()
    }

    /// Components of `chosen` under "shares any binding key". Discovered in
    /// ascending-minimum order, matching the Python union-find's grouping.
    fn components(&self, chosen: &Bits) -> Vec<Bits> {
        let mut assigned = Bits::new(self.n_nodes);
        let mut components: Vec<Bits> = Vec::new();
        for start in chosen.ones() {
            if assigned.contains(start) {
                continue;
            }
            let mut component = Bits::new(self.n_nodes);
            component.insert(start);
            assigned.insert(start);
            let mut stack = vec![start];
            while let Some(current) = stack.pop() {
                for partner in self.join_partners[current].ones() {
                    if chosen.contains(partner) && !assigned.contains(partner) {
                        assigned.insert(partner);
                        component.insert(partner);
                        stack.push(partner);
                    }
                }
            }
            components.push(component);
        }
        components
    }

    /// `_label_chain_state`: walk the in-cover functional chains off `source`;
    /// labeled when the walk reaches an in-cover full binder of the terminal,
    /// otherwise the satisfiers are the frontier completers one hop from a
    /// row-complete walked origin.
    fn label_chain(&self, source: usize, position: usize, chosen: &Bits) -> Option<Vec<u32>> {
        let full = &self.full_binder_bits[position];
        let mut walked = Bits::new(self.n_nodes);
        walked.insert(source);
        let mut origins = Bits::new(self.n_nodes);
        origins.insert(source);
        let mut stack = vec![source];
        while let Some(current) = stack.pop() {
            for node in self.func_succ[current].ones() {
                if walked.contains(node) || !chosen.contains(node) {
                    continue;
                }
                if full.contains(node) {
                    return None;
                }
                walked.insert(node);
                if self.row_complete[node] {
                    origins.insert(node);
                    stack.push(node);
                }
            }
        }
        let frontier: Vec<u32> = self.chain_completers[position]
            .ones()
            .filter(|node| !chosen.contains(*node) && self.func_pred[*node].intersects(&origins))
            .map(|node| node as u32)
            .collect();
        Some(frontier)
    }

    /// `compute_pending_obligations` + `prune_subsumed_arms`, reduced to what
    /// the walk reads: whether anything is pending, and the minimum obligation
    /// by `(len(satisfiers), identity)`.
    fn scarcest_pending(&self, chosen: &Bits) -> Option<Ob> {
        let mut best: Option<Ob> = None;
        let mut consider = |candidate: Ob| {
            match &best {
                Some(current) if current.key() <= candidate.key() => {}
                _ => best = Some(candidate),
            }
        };
        // cover
        for (position, terminal) in self.terminals.iter().enumerate() {
            if self.binder_bits[position].intersects(chosen) {
                continue;
            }
            if self.cover_satisfiers[position].is_empty() {
                continue;
            }
            consider(Ob {
                kind: Kind::Cover,
                subject: vec![*terminal],
                satisfiers: self.cover_satisfiers[position].clone(),
            });
        }
        // axis — the one kind Python mints without a non-empty satisfier
        // guard; an (authored-empty) member would kill the state, so the
        // empty list must survive to the branch step here too.
        for family in &self.axis_families {
            for member in &family.members {
                if !member.bits.intersects(chosen) {
                    consider(Ob {
                        kind: Kind::Axis,
                        subject: vec![family.representative, member.subject_rank],
                        satisfiers: prune_arms(&self.subsumed, member.nodes.clone()),
                    });
                }
            }
        }
        // paired
        for side in &self.sides {
            if !side.carrier_bits.intersects(chosen)
                || side.materializer_bits.intersects(chosen)
                || side.satisfiers.is_empty()
            {
                continue;
            }
            consider(Ob {
                kind: Kind::Paired,
                subject: side.subject.clone(),
                satisfiers: side.satisfiers.clone(),
            });
        }
        let chosen_count = chosen.count();
        for source in chosen.ones() {
            // labelable
            if !self.bound_terminals[source].is_empty() {
                for (position, terminal) in self.terminals.iter().enumerate() {
                    if self.bound_terminals[source].contains(position)
                        || !self.chain_completers[position].contains(source)
                    {
                        continue;
                    }
                    if let Some(frontier) = self.label_chain(source, position, chosen) {
                        let satisfiers = prune_arms(&self.subsumed, frontier);
                        if !satisfiers.is_empty() {
                            consider(Ob {
                                kind: Kind::Labelable,
                                subject: vec![source as u32, *terminal],
                                satisfiers,
                            });
                        }
                    }
                }
            }
            // colocated
            if chosen_count >= 2 && self.grain_nonempty[source] {
                let mut others = chosen.clone();
                others.words[source / 64] &= !(1u64 << (source % 64));
                if !self.functional_partners[source].intersects(&others) {
                    let mut extras: Vec<u32> = (0..self.n_nodes)
                        .filter(|extra| {
                            !chosen.contains(*extra)
                                && self.grain[source].is_subset(&self.binding_keys[*extra])
                                && self.functional_partners[*extra].intersects(&others)
                        })
                        .map(|extra| extra as u32)
                        .collect();
                    extras.sort_by_key(|extra| {
                        (self.binding_keys[*extra as usize].count(), *extra)
                    });
                    let satisfiers = prune_arms(&self.subsumed, extras);
                    if !satisfiers.is_empty() {
                        consider(Ob {
                            kind: Kind::Colocated,
                            subject: vec![source as u32],
                            satisfiers,
                        });
                    }
                }
            }
        }
        // connected: deliberately last and only when nothing else is pending.
        if best.is_none() && chosen_count > 1 {
            let components = self.components(chosen);
            if components.len() > 1 {
                let mut mergers: Vec<u32> = Vec::new();
                let mut adjacent: Vec<u32> = Vec::new();
                for node in 0..self.n_nodes {
                    if chosen.contains(node) {
                        continue;
                    }
                    let touched = components
                        .iter()
                        .filter(|component| self.join_partners[node].intersects(component))
                        .count();
                    if touched >= 1 {
                        adjacent.push(node as u32);
                    }
                    if touched >= 2 {
                        mergers.push(node as u32);
                    }
                }
                let satisfiers = prune_arms(
                    &self.subsumed,
                    if mergers.is_empty() { adjacent } else { mergers },
                );
                if !satisfiers.is_empty() {
                    let mut subject: Vec<u32> = components
                        .iter()
                        .map(|component| component.ones().next().unwrap() as u32)
                        .collect();
                    subject.sort();
                    best = Some(Ob {
                        kind: Kind::Connected,
                        subject,
                        satisfiers,
                    });
                }
            }
        }
        best
    }
}

/// `prune_subsumed_arms` for one satisfier list: drop an arm whose subsuming
/// union is ALSO offered for the same obligation.
fn prune_arms(subsumed: &[Option<u32>], satisfiers: Vec<u32>) -> Vec<u32> {
    if subsumed.iter().all(|entry| entry.is_none()) {
        return satisfiers;
    }
    let present: HashSet<u32> = satisfiers.iter().copied().collect();
    let kept: Vec<u32> = satisfiers
        .iter()
        .copied()
        .filter(|node| match subsumed[*node as usize] {
            Some(union_node) => !present.contains(&union_node),
            None => true,
        })
        .collect();
    kept
}

/// `_enumerate_covers`: the level-order obligation walk. Returns the emitted
/// covers (as node names, in emission order) and which budget truncated the
/// walk, if any.
pub fn enumerate_covers(spec: &NetworkSpec) -> (Vec<Vec<String>>, Option<LimitKind>) {
    let net = Net::build(spec);
    let mut covers: Vec<Bits> = Vec::new();
    let mut emitted: Vec<(Bits, Vec<u8>)> = Vec::new();
    let mut visited: HashSet<Bits> = HashSet::new();
    let mut level: Vec<Bits> = vec![Bits::new(net.n_nodes)];
    let mut limit: Option<LimitKind> = None;
    'walk: while !level.is_empty() {
        // Within a level, first-pushed pops first: the push order fixes which
        // covers survive truncation, so it must stay deterministic.
        let mut next_level: Vec<Bits> = Vec::new();
        for chosen in level {
            if visited.contains(&chosen) {
                continue;
            }
            if covers.len() >= spec.cover_limit {
                limit = Some(LimitKind::Covers);
                break 'walk;
            }
            if visited.len() >= spec.state_limit {
                limit = Some(LimitKind::States);
                break 'walk;
            }
            visited.insert(chosen.clone());
            if !emitted.is_empty() {
                let profile = net.profile(&chosen);
                if emitted.iter().any(|(prior, prior_profile)| {
                    prior.is_proper_subset(&chosen) && *prior_profile == profile
                }) {
                    continue;
                }
            }
            if let Some(first) = net.scarcest_pending(&chosen) {
                for node in &first.satisfiers {
                    next_level.push(chosen.with(*node as usize));
                }
                continue;
            }
            let profile = net.profile(&chosen);
            for (position, _) in net.terminals.iter().enumerate() {
                if net.full_binder_bits[position].intersects(&chosen) {
                    continue;
                }
                // Soft branch: only nodes that are BOTH binders and full
                // binders — `binders` order is ascending, like the Python sort.
                for node in &net.full_binders[position] {
                    if net.binders[position].contains(node) {
                        next_level.push(chosen.with(*node as usize));
                    }
                }
            }
            covers.push(chosen.clone());
            emitted.push((chosen, profile));
        }
        level = next_level;
    }
    let names: Vec<Vec<String>> = covers
        .iter()
        .map(|cover| {
            cover
                .ones()
                .map(|node| net.node_names[node].clone())
                .collect()
        })
        .collect();
    (names, limit)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(node: &str, bindings: &[(&str, bool)], grain: &[&str]) -> CandidateSpec {
        CandidateSpec {
            node: node.to_string(),
            bindings: bindings
                .iter()
                .map(|(a, p)| (a.to_string(), *p))
                .collect(),
            grain: grain.iter().map(|g| g.to_string()).collect(),
            partial_is_full: false,
        }
    }

    fn spec(terminals: &[&str], candidates: Vec<CandidateSpec>) -> NetworkSpec {
        NetworkSpec {
            terminals: terminals.iter().map(|t| t.to_string()).collect(),
            candidates,
            axis_families: vec![],
            join_requirements: vec![],
            subsumed_arms: vec![],
            cover_limit: 4096,
            state_limit: 10_000,
        }
    }

    #[test]
    fn single_source_cover() {
        let s = spec(
            &["a", "b"],
            vec![candidate("ds~one", &[("a", false), ("b", false)], &["a"])],
        );
        let (covers, limit) = enumerate_covers(&s);
        assert_eq!(limit, None);
        assert_eq!(covers, vec![vec!["ds~one".to_string()]]);
    }

    #[test]
    fn state_limit_reports_states() {
        let s = NetworkSpec {
            state_limit: 1,
            ..spec(
                &["a", "b"],
                vec![
                    candidate("ds~one", &[("a", false), ("k", false)], &["a"]),
                    candidate("ds~two", &[("b", false), ("k", false)], &["b"]),
                ],
            )
        };
        let (covers, limit) = enumerate_covers(&s);
        assert_eq!(limit, Some(LimitKind::States));
        assert!(covers.is_empty());
    }

    #[test]
    fn soft_branch_emits_full_binder_upgrade() {
        // `ds~launch` binds `name` only partially; the walk emits the launch
        // cover, then the soft branch adds the full binder as a second cover.
        let s = spec(
            &["launch", "name"],
            vec![
                candidate(
                    "ds~launch",
                    &[("launch", false), ("vehicle", false), ("name", true)],
                    &["launch"],
                ),
                candidate(
                    "ds~vehicle",
                    &[("vehicle", false), ("name", false)],
                    &["vehicle"],
                ),
            ],
        );
        let (covers, limit) = enumerate_covers(&s);
        assert_eq!(limit, None);
        assert_eq!(
            covers,
            vec![
                vec!["ds~launch".to_string()],
                vec!["ds~launch".to_string(), "ds~vehicle".to_string()],
            ]
        );
    }

    #[test]
    fn subsumed_arm_branches_only_onto_the_union() {
        let mut s = spec(
            &["k"],
            vec![
                candidate("ds~arm_a", &[("k", true)], &["k"]),
                candidate("ds~arm_b", &[("k", true)], &["k"]),
                candidate("ds~union", &[("k", false)], &["k"]),
            ],
        );
        s.subsumed_arms = vec![
            ("ds~arm_a".to_string(), "ds~union".to_string()),
            ("ds~arm_b".to_string(), "ds~union".to_string()),
        ];
        let (covers, limit) = enumerate_covers(&s);
        assert_eq!(limit, None);
        assert_eq!(covers, vec![vec!["ds~union".to_string()]]);
    }

    #[test]
    fn disconnected_cover_is_bridged() {
        let s = spec(
            &["a_val", "b_val"],
            vec![
                candidate("ds~fact_a", &[("a_id", false), ("sk", false), ("a_val", false)], &["a_id"]),
                candidate("ds~fact_b", &[("b_id", false), ("ok", false), ("b_val", false)], &["b_id"]),
                candidate("ds~bridge", &[("sk", false), ("ok", false)], &["sk", "ok"]),
            ],
        );
        let (covers, limit) = enumerate_covers(&s);
        assert_eq!(limit, None);
        assert!(covers.iter().any(|cover| cover.len() == 3));
        // no emitted cover is the disconnected two-fact pair
        assert!(!covers.contains(&vec![
            "ds~fact_a".to_string(),
            "ds~fact_b".to_string()
        ]));
    }

    #[test]
    fn empty_terminals_emit_the_empty_cover() {
        let s = spec(&[], vec![candidate("ds~one", &[("a", false)], &["a"])]);
        let (covers, limit) = enumerate_covers(&s);
        assert_eq!(limit, None);
        assert_eq!(covers, vec![Vec::<String>::new()]);
    }
}
