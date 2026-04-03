use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};

const FLOAT_TOLERANCE: f64 = 1e-9;

#[derive(Clone, Default)]
pub struct GraphCore {
    directed: bool,
    succ: HashMap<String, HashSet<String>>,
    pred: HashMap<String, HashSet<String>>,
    node_order: Vec<String>,
    succ_order: HashMap<String, Vec<String>>,
    pred_order: HashMap<String, Vec<String>>,
}

#[derive(Clone)]
struct State {
    cost: f64,
    node: String,
}

impl Eq for State {}

impl PartialEq for State {
    fn eq(&self, other: &Self) -> bool {
        self.cost.total_cmp(&other.cost) == Ordering::Equal && self.node == other.node
    }
}

impl Ord for State {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .cost
            .total_cmp(&self.cost)
            .then_with(|| other.node.cmp(&self.node))
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Copy, Clone)]
struct IndexState {
    cost: f64,
    node: usize,
}

impl Eq for IndexState {}

impl PartialEq for IndexState {
    fn eq(&self, other: &Self) -> bool {
        self.cost.total_cmp(&other.cost) == Ordering::Equal && self.node == other.node
    }
}

impl Ord for IndexState {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .cost
            .total_cmp(&self.cost)
            .then_with(|| other.node.cmp(&self.node))
    }
}

impl PartialOrd for IndexState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Default)]
struct DisjointSet {
    parent: HashMap<String, String>,
    rank: HashMap<String, usize>,
}

impl DisjointSet {
    fn insert(&mut self, node: &str) {
        self.parent
            .entry(node.to_string())
            .or_insert_with(|| node.to_string());
        self.rank.entry(node.to_string()).or_insert(0);
    }

    fn find(&mut self, node: &str) -> String {
        let parent = self
            .parent
            .get(node)
            .cloned()
            .unwrap_or_else(|| node.to_string());
        if parent == node {
            self.insert(node);
            return parent;
        }
        let root = self.find(&parent);
        self.parent.insert(node.to_string(), root.clone());
        root
    }

    fn union(&mut self, left: &str, right: &str) -> bool {
        self.insert(left);
        self.insert(right);
        let left_root = self.find(left);
        let right_root = self.find(right);
        if left_root == right_root {
            return false;
        }
        let left_rank = *self.rank.get(&left_root).unwrap_or(&0);
        let right_rank = *self.rank.get(&right_root).unwrap_or(&0);
        if left_rank < right_rank {
            self.parent.insert(left_root, right_root);
        } else if left_rank > right_rank {
            self.parent.insert(right_root, left_root);
        } else {
            self.parent.insert(right_root.clone(), left_root.clone());
            self.rank.insert(left_root, left_rank + 1);
        }
        true
    }
}

impl GraphCore {
    pub fn new(directed: bool) -> Self {
        Self {
            directed,
            succ: HashMap::new(),
            pred: HashMap::new(),
            node_order: Vec::new(),
            succ_order: HashMap::new(),
            pred_order: HashMap::new(),
        }
    }

    pub fn directed(&self) -> bool {
        self.directed
    }

    pub fn add_node(&mut self, node: &str) {
        if self.succ.contains_key(node) {
            return;
        }
        let owned = node.to_string();
        self.node_order.push(owned.clone());
        self.succ.insert(owned.clone(), HashSet::new());
        self.pred.insert(owned.clone(), HashSet::new());
        self.succ_order.insert(owned.clone(), Vec::new());
        self.pred_order.insert(owned, Vec::new());
    }

    pub fn has_node(&self, node: &str) -> bool {
        self.succ.contains_key(node)
    }

    pub fn add_edge(&mut self, left: &str, right: &str) {
        self.add_node(left);
        self.add_node(right);

        if let Some(neighbors) = self.succ.get_mut(left) {
            if neighbors.insert(right.to_string()) {
                self.succ_order
                    .entry(left.to_string())
                    .or_default()
                    .push(right.to_string());
            }
        }
        if let Some(neighbors) = self.pred.get_mut(right) {
            if neighbors.insert(left.to_string()) {
                self.pred_order
                    .entry(right.to_string())
                    .or_default()
                    .push(left.to_string());
            }
        }

        if !self.directed {
            if let Some(neighbors) = self.succ.get_mut(right) {
                if neighbors.insert(left.to_string()) {
                    self.succ_order
                        .entry(right.to_string())
                        .or_default()
                        .push(left.to_string());
                }
            }
            if let Some(neighbors) = self.pred.get_mut(left) {
                if neighbors.insert(right.to_string()) {
                    self.pred_order
                        .entry(left.to_string())
                        .or_default()
                        .push(right.to_string());
                }
            }
        }
    }

    pub fn has_edge(&self, left: &str, right: &str) -> bool {
        self.succ
            .get(left)
            .map(|neighbors| neighbors.contains(right))
            .unwrap_or(false)
    }

    pub fn remove_edge(&mut self, left: &str, right: &str) {
        if let Some(neighbors) = self.succ.get_mut(left) {
            if neighbors.remove(right) {
                remove_ordered_neighbor(&mut self.succ_order, left, right);
            }
        }
        if let Some(neighbors) = self.pred.get_mut(right) {
            if neighbors.remove(left) {
                remove_ordered_neighbor(&mut self.pred_order, right, left);
            }
        }
        if !self.directed {
            if let Some(neighbors) = self.succ.get_mut(right) {
                if neighbors.remove(left) {
                    remove_ordered_neighbor(&mut self.succ_order, right, left);
                }
            }
            if let Some(neighbors) = self.pred.get_mut(left) {
                if neighbors.remove(right) {
                    remove_ordered_neighbor(&mut self.pred_order, left, right);
                }
            }
        }
    }

    pub fn remove_edges(&mut self, edges: Vec<(String, String)>) {
        for (left, right) in edges {
            self.remove_edge(&left, &right);
        }
    }

    pub fn remove_node(&mut self, node: &str) {
        let outgoing = self.successors(node);
        let incoming = self.predecessors(node);

        for neighbor in outgoing {
            if let Some(preds) = self.pred.get_mut(&neighbor) {
                if preds.remove(node) {
                    remove_ordered_neighbor(&mut self.pred_order, &neighbor, node);
                }
            }
            if !self.directed {
                if let Some(succs) = self.succ.get_mut(&neighbor) {
                    if succs.remove(node) {
                        remove_ordered_neighbor(&mut self.succ_order, &neighbor, node);
                    }
                }
            }
        }

        for neighbor in incoming {
            if let Some(succs) = self.succ.get_mut(&neighbor) {
                if succs.remove(node) {
                    remove_ordered_neighbor(&mut self.succ_order, &neighbor, node);
                }
            }
            if !self.directed {
                if let Some(preds) = self.pred.get_mut(&neighbor) {
                    if preds.remove(node) {
                        remove_ordered_neighbor(&mut self.pred_order, &neighbor, node);
                    }
                }
            }
        }

        self.succ.remove(node);
        self.pred.remove(node);
        self.succ_order.remove(node);
        self.pred_order.remove(node);
        self.node_order.retain(|existing| existing != node);
    }

    pub fn remove_nodes(&mut self, nodes: Vec<String>) {
        for node in nodes {
            self.remove_node(&node);
        }
    }

    pub fn nodes(&self) -> Vec<String> {
        self.node_order.clone()
    }

    pub fn edges(&self) -> Vec<(String, String)> {
        let mut edges = Vec::new();
        let mut seen = HashSet::new();
        for left in self.nodes() {
            if let Some(neighbors) = self.succ_order.get(&left) {
                for right in neighbors {
                    if !self.has_edge(&left, right) {
                        continue;
                    }
                    if self.directed {
                        edges.push((left.clone(), right.clone()));
                    } else {
                        let key = canonical_edge(&left, right);
                        if seen.insert(key) {
                            edges.push((left.clone(), right.clone()));
                        }
                    }
                }
            }
        }
        edges
    }

    pub fn neighbors(&self, node: &str) -> Vec<String> {
        self.succ_order
            .get(node)
            .map(|neighbors| {
                neighbors
                    .iter()
                    .filter(|neighbor| self.has_edge(node, neighbor))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn predecessors(&self, node: &str) -> Vec<String> {
        self.pred_order
            .get(node)
            .map(|neighbors| {
                neighbors
                    .iter()
                    .filter(|neighbor| self.has_edge(neighbor, node))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn successors(&self, node: &str) -> Vec<String> {
        self.neighbors(node)
    }

    pub fn all_neighbors(&self, node: &str) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut output = Vec::new();
        for neighbor in self.predecessors(node) {
            if seen.insert(neighbor.clone()) {
                output.push(neighbor);
            }
        }
        for neighbor in self.successors(node) {
            if seen.insert(neighbor.clone()) {
                output.push(neighbor);
            }
        }
        output
    }

    pub fn in_degree(&self, node: &str) -> usize {
        self.pred.get(node).map(|neighbors| neighbors.len()).unwrap_or(0)
    }

    pub fn out_degree(&self, node: &str) -> usize {
        self.succ.get(node).map(|neighbors| neighbors.len()).unwrap_or(0)
    }

    pub fn clone_graph(&self) -> Self {
        self.clone()
    }

    pub fn induced_subgraph(&self, nodes: Vec<String>) -> Self {
        let keep = nodes.into_iter().collect::<HashSet<_>>();
        let mut graph = Self::new(self.directed);
        for node in self.nodes() {
            if keep.contains(&node) {
                graph.add_node(&node);
            }
        }
        for (left, right) in self.edges() {
            if keep.contains(&left) && keep.contains(&right) {
                graph.add_edge(&left, &right);
            }
        }
        graph
    }

    pub fn to_undirected_graph(&self) -> Self {
        let mut graph = Self::new(false);
        for node in self.nodes() {
            graph.add_node(&node);
        }
        for (left, right) in self.edges() {
            graph.add_edge(&left, &right);
        }
        graph
    }

    pub fn connected_components(&self) -> Vec<Vec<String>> {
        let mut seen = HashSet::new();
        let mut components = Vec::new();

        for node in self.nodes() {
            if seen.contains(&node) {
                continue;
            }
            let mut queue = VecDeque::from([node.clone()]);
            let mut component = Vec::new();
            seen.insert(node.clone());
            while let Some(current) = queue.pop_front() {
                component.push(current.clone());
                for neighbor in self.all_neighbors(&current) {
                    if seen.insert(neighbor.clone()) {
                        queue.push_back(neighbor);
                    }
                }
            }
            components.push(component);
        }
        components
    }

    pub fn is_weakly_connected(&self) -> bool {
        let node_count = self.succ.len();
        if node_count <= 1 {
            return true;
        }
        self.connected_components().len() == 1
    }

    pub fn topological_sort(&self) -> Result<Vec<String>, String> {
        let mut indegree = HashMap::new();
        for node in self.nodes() {
            indegree.insert(node.clone(), self.in_degree(&node));
        }

        let mut ready = VecDeque::new();
        for node in self.nodes() {
            if indegree.get(&node) == Some(&0usize) {
                ready.push_back(node);
            }
        }

        let mut output = Vec::new();
        while let Some(node) = ready.pop_front() {
            output.push(node.clone());
            for neighbor in self.successors(&node) {
                if let Some(entry) = indegree.get_mut(&neighbor) {
                    *entry -= 1;
                    if *entry == 0 {
                        ready.push_back(neighbor);
                    }
                }
            }
        }

        if output.len() != self.succ.len() {
            return Err("Graph contains a cycle".to_string());
        }
        Ok(output)
    }

    pub fn shortest_path(&self, source: &str, target: &str) -> Option<Vec<String>> {
        if !self.has_node(source) || !self.has_node(target) {
            return None;
        }
        if source == target {
            return Some(vec![source.to_string()]);
        }

        let mut queue = VecDeque::from([source.to_string()]);
        let mut visited = HashSet::from([source.to_string()]);
        let mut previous: HashMap<String, String> = HashMap::new();

        while let Some(current) = queue.pop_front() {
            for neighbor in self.successors(&current) {
                if !visited.insert(neighbor.clone()) {
                    continue;
                }
                previous.insert(neighbor.clone(), current.clone());
                if neighbor == target {
                    return Some(reconstruct_path(&previous, source, target));
                }
                queue.push_back(neighbor);
            }
        }

        None
    }

    pub fn shortest_path_length(&self, source: &str, target: &str) -> Option<usize> {
        self.shortest_path(source, target)
            .map(|path| path.len().saturating_sub(1))
    }

    pub fn ego_graph_nodes(&self, center: &str, radius: usize) -> Vec<String> {
        if !self.has_node(center) {
            return Vec::new();
        }

        let mut queue = VecDeque::from([(center.to_string(), 0usize)]);
        let mut visited = HashSet::from([center.to_string()]);

        while let Some((current, depth)) = queue.pop_front() {
            if depth >= radius {
                continue;
            }
            for neighbor in self.successors(&current) {
                if visited.insert(neighbor.clone()) {
                    queue.push_back((neighbor, depth + 1));
                }
            }
        }

        self.nodes()
            .into_iter()
            .filter(|node| visited.contains(node))
            .collect()
    }

    pub fn multi_source_dijkstra_path(
        &self,
        sources: Vec<String>,
        weights: Vec<(String, String, f64)>,
    ) -> Result<Vec<(String, Vec<String>)>, String> {
        let source_set = dedupe_preserve_order(sources);
        if source_set.is_empty() {
            return Ok(Vec::new());
        }
        for source in &source_set {
            if !self.has_node(source) {
                return Err(format!("Node not found: {source}"));
            }
        }
        let weight_map = self.weight_map(weights);

        let mut heap = BinaryHeap::new();
        let mut distances: HashMap<String, f64> = HashMap::new();
        let mut paths: HashMap<String, Vec<String>> = HashMap::new();

        for source in &source_set {
            distances.insert(source.clone(), 0.0);
            paths.insert(source.clone(), vec![source.clone()]);
            heap.push(State {
                cost: 0.0,
                node: source.clone(),
            });
        }

        while let Some(State { cost, node }) = heap.pop() {
            let Some(best_cost) = distances.get(&node) else {
                continue;
            };
            if cost > *best_cost + FLOAT_TOLERANCE {
                continue;
            }
            let Some(current_path) = paths.get(&node).cloned() else {
                continue;
            };

            let Some(neighbors) = self.succ_order.get(&node) else {
                continue;
            };
            for neighbor in neighbors {
                if !self.has_edge(&node, neighbor) {
                    continue;
                }
                let edge_weight = self.edge_weight(&node, neighbor, &weight_map);
                let next_cost = cost + edge_weight;
                let mut next_path = current_path.clone();
                next_path.push(neighbor.clone());

                let should_update = match distances.get(neighbor) {
                    None => true,
                    Some(existing) if next_cost + FLOAT_TOLERANCE < *existing => true,
                    Some(existing)
                        if (next_cost - *existing).abs() <= FLOAT_TOLERANCE =>
                    {
                        match paths.get(neighbor) {
                            None => true,
                            Some(existing_path) => next_path < *existing_path,
                        }
                    }
                    _ => false,
                };

                if should_update {
                    distances.insert(neighbor.clone(), next_cost);
                    paths.insert(neighbor.clone(), next_path);
                    heap.push(State {
                        cost: next_cost,
                        node: neighbor.clone(),
                    });
                }
            }
        }

        let mut output = Vec::new();
        for node in self.nodes() {
            if let Some(path) = paths.remove(&node) {
                output.push((node, path));
            }
        }
        Ok(output)
    }

    pub fn steiner_tree_nodes(
        &self,
        terminals: Vec<String>,
        weights: Vec<(String, String, f64)>,
    ) -> Result<Vec<String>, String> {
        self.steiner_tree_nodes_indexed(terminals, weights)
    }

    pub fn steiner_tree_nodes_legacy(
        &self,
        terminals: Vec<String>,
        weights: Vec<(String, String, f64)>,
    ) -> Result<Vec<String>, String> {
        let terminals = dedupe_preserve_order(terminals);
        if terminals.is_empty() {
            return Ok(Vec::new());
        }
        for terminal in &terminals {
            if !self.has_node(terminal) {
                return Err(format!("Node not found: {terminal}"));
            }
        }
        if terminals.len() == 1 {
            return Ok(terminals);
        }

        let weight_map = self.weight_map(weights);
        let mut metric_edges: Vec<(f64, String, String, Vec<String>)> = Vec::new();
        for index in 0..terminals.len() {
            let left = &terminals[index];
            let targets = terminals[index + 1..].to_vec();
            let shortest_paths =
                self.weighted_shortest_paths_to_targets(left, &targets, &weight_map);
            for other_index in index + 1..terminals.len() {
                let right = &terminals[other_index];
                let Some((distance, path)) = shortest_paths.get(right) else {
                    return Err(format!("No path between {left} and {right}"));
                };
                metric_edges.push(( *distance, left.clone(), right.clone(), path.clone()));
            }
        }

        metric_edges.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
                .then_with(|| left.2.cmp(&right.2))
        });

        let mut metric_dsu = DisjointSet::default();
        let mut expanded_nodes = terminals.iter().cloned().collect::<HashSet<_>>();
        for terminal in &terminals {
            metric_dsu.insert(terminal);
        }
        for (_distance, left, right, path) in metric_edges {
            if metric_dsu.union(&left, &right) {
                expanded_nodes.extend(path);
            }
        }

        Ok(self.finalize_steiner_tree(terminals, expanded_nodes, &weight_map))
    }

    fn finalize_steiner_tree(
        &self,
        terminals: Vec<String>,
        expanded_nodes: HashSet<String>,
        weight_map: &HashMap<String, HashMap<String, f64>>,
    ) -> Vec<String> {
        let induced = expanded_nodes.clone();
        let ordered_expanded_nodes = self
            .nodes()
            .into_iter()
            .filter(|node| induced.contains(node))
            .collect::<Vec<_>>();
        let mut original_edges = Vec::new();
        let mut seen_edges = HashSet::new();
        for left in &ordered_expanded_nodes {
            let Some(neighbors) = self.succ_order.get(left) else {
                continue;
            };
            for right in neighbors {
                if !induced.contains(right) || !self.has_edge(left, right) {
                    continue;
                }
                let key = canonical_edge(left, right);
                if self.directed || seen_edges.insert(key) {
                    original_edges.push((
                        self.edge_weight(left, right, &weight_map),
                        left.clone(),
                        right.clone(),
                    ));
                }
            }
        }
        original_edges.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
                .then_with(|| left.2.cmp(&right.2))
        });

        let mut tree = HashMap::<String, HashSet<String>>::new();
        let mut induced_dsu = DisjointSet::default();
        for node in &ordered_expanded_nodes {
            tree.entry(node.clone()).or_default();
            induced_dsu.insert(node);
        }
        for (_weight, left, right) in original_edges {
            if induced_dsu.union(&left, &right) {
                tree.entry(left.clone()).or_default().insert(right.clone());
                tree.entry(right).or_default().insert(left);
            }
        }

        let terminals_set = terminals.into_iter().collect::<HashSet<_>>();
        let mut removable = ordered_expanded_nodes
            .iter()
            .filter_map(|node| {
                let neighbors = tree.get(node)?;
                if !terminals_set.contains(node) && neighbors.len() <= 1 {
                    Some(node.clone())
                } else {
                    None
                }
            })
            .collect::<VecDeque<_>>();
        while let Some(node) = removable.pop_front() {
            let Some(neighbors) = tree.remove(&node) else {
                continue;
            };
            for neighbor in neighbors {
                if let Some(entries) = tree.get_mut(&neighbor) {
                    entries.remove(&node);
                    if !terminals_set.contains(&neighbor) && entries.len() <= 1 {
                        removable.push_back(neighbor.clone());
                    }
                }
            }
        }

        self.nodes()
            .into_iter()
            .filter(|node| tree.contains_key(node))
            .collect()
    }

    fn steiner_tree_nodes_indexed(
        &self,
        terminals: Vec<String>,
        weights: Vec<(String, String, f64)>,
    ) -> Result<Vec<String>, String> {
        let terminals = dedupe_preserve_order(terminals);
        if terminals.is_empty() {
            return Ok(Vec::new());
        }
        let lex_nodes = {
            let mut nodes = self.nodes();
            nodes.sort();
            nodes
        };
        let mut node_to_index = HashMap::new();
        for (index, node) in lex_nodes.iter().enumerate() {
            node_to_index.insert(node.clone(), index);
        }
        let mut terminal_indices = Vec::with_capacity(terminals.len());
        for terminal in &terminals {
            let Some(index) = node_to_index.get(terminal).copied() else {
                return Err(format!("Node not found: {terminal}"));
            };
            terminal_indices.push(index);
        }
        if terminal_indices.len() == 1 {
            return Ok(terminals);
        }

        let weight_map = self.weight_map(weights);
        let mut weighted_neighbors = vec![Vec::<(usize, f64)>::new(); lex_nodes.len()];
        for (index, left) in lex_nodes.iter().enumerate() {
            let Some(neighbors) = self.succ_order.get(left) else {
                continue;
            };
            for right in neighbors {
                if !self.has_edge(left, right) {
                    continue;
                }
                let Some(right_index) = node_to_index.get(right).copied() else {
                    continue;
                };
                weighted_neighbors[index]
                    .push((right_index, self.edge_weight(left, right, &weight_map)));
            }
        }

        let mut metric_edges: Vec<(f64, usize, usize, Vec<usize>)> = Vec::new();
        for index in 0..terminal_indices.len() {
            let left = terminal_indices[index];
            let targets = terminal_indices[index + 1..].to_vec();
            let shortest_paths = self.weighted_shortest_paths_to_target_indices(
                left,
                &targets,
                &weighted_neighbors,
            );
            for other_index in index + 1..terminal_indices.len() {
                let right = terminal_indices[other_index];
                let Some((distance, path)) = shortest_paths.get(&right) else {
                    return Err(format!(
                        "No path between {} and {}",
                        terminals[index], terminals[other_index]
                    ));
                };
                metric_edges.push((*distance, left, right, path.clone()));
            }
        }

        metric_edges.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
                .then_with(|| left.2.cmp(&right.2))
        });

        let mut metric_dsu = IndexDisjointSet::new(lex_nodes.len());
        let mut expanded_nodes = terminals.iter().cloned().collect::<HashSet<_>>();
        for (_distance, left, right, path) in metric_edges {
            if metric_dsu.union(left, right) {
                for node in path {
                    expanded_nodes.insert(lex_nodes[node].clone());
                }
            }
        }

        Ok(self.finalize_steiner_tree(terminals, expanded_nodes, &weight_map))
    }

    fn weight_map(&self, weights: Vec<(String, String, f64)>) -> HashMap<String, HashMap<String, f64>> {
        let mut output = HashMap::new();
        for (left, right, weight) in weights {
            output
                .entry(left.clone())
                .or_insert_with(HashMap::new)
                .insert(right.clone(), weight);
            if !self.directed {
                output
                    .entry(right)
                    .or_insert_with(HashMap::new)
                    .insert(left, weight);
            }
        }
        output
    }

    fn edge_weight(
        &self,
        left: &str,
        right: &str,
        weights: &HashMap<String, HashMap<String, f64>>,
    ) -> f64 {
        weights
            .get(left)
            .and_then(|neighbors| neighbors.get(right))
            .copied()
            .unwrap_or(1.0)
    }

    fn weighted_shortest_paths_to_targets(
        &self,
        source: &str,
        targets: &[String],
        weights: &HashMap<String, HashMap<String, f64>>,
    ) -> HashMap<String, (f64, Vec<String>)> {
        if !self.has_node(source) {
            return HashMap::new();
        }
        let mut remaining = targets.iter().cloned().collect::<HashSet<_>>();
        if remaining.is_empty() {
            return HashMap::new();
        }

        let mut heap = BinaryHeap::new();
        let mut distances: HashMap<String, f64> =
            HashMap::from([(source.to_string(), 0.0)]);
        let mut paths: HashMap<String, Vec<String>> =
            HashMap::from([(source.to_string(), vec![source.to_string()])]);
        heap.push(State {
            cost: 0.0,
            node: source.to_string(),
        });

        while let Some(State { cost, node }) = heap.pop() {
            let Some(best_cost) = distances.get(&node) else {
                continue;
            };
            if cost > *best_cost + FLOAT_TOLERANCE {
                continue;
            }
            remaining.remove(&node);
            if remaining.is_empty() {
                break;
            }
            let Some(current_path) = paths.get(&node).cloned() else {
                continue;
            };

            let Some(neighbors) = self.succ_order.get(&node) else {
                continue;
            };
            for neighbor in neighbors {
                if !self.has_edge(&node, neighbor) {
                    continue;
                }
                let next_cost = cost + self.edge_weight(&node, neighbor, weights);
                let mut next_path = current_path.clone();
                next_path.push(neighbor.clone());

                let should_update = match distances.get(neighbor) {
                    None => true,
                    Some(existing) if next_cost + FLOAT_TOLERANCE < *existing => true,
                    Some(existing)
                        if (next_cost - *existing).abs() <= FLOAT_TOLERANCE =>
                    {
                        match paths.get(neighbor) {
                            None => true,
                            Some(existing_path) => next_path < *existing_path,
                        }
                    }
                    _ => false,
                };

                if should_update {
                    distances.insert(neighbor.clone(), next_cost);
                    paths.insert(neighbor.clone(), next_path);
                    heap.push(State {
                        cost: next_cost,
                        node: neighbor.clone(),
                    });
                }
            }
        }

        targets
            .iter()
            .into_iter()
            .filter_map(|node| {
                let distance = distances.get(node).copied()?;
                let path = paths.remove(node)?;
                Some((node.clone(), (distance, path)))
            })
            .collect()
    }

    fn weighted_shortest_paths_to_target_indices(
        &self,
        source: usize,
        targets: &[usize],
        weighted_neighbors: &[Vec<(usize, f64)>],
    ) -> HashMap<usize, (f64, Vec<usize>)> {
        if targets.is_empty() {
            return HashMap::new();
        }
        let mut remaining = targets.iter().copied().collect::<HashSet<_>>();
        let mut heap = BinaryHeap::new();
        let mut distances = vec![f64::INFINITY; weighted_neighbors.len()];
        let mut paths: Vec<Option<Vec<usize>>> = vec![None; weighted_neighbors.len()];

        distances[source] = 0.0;
        paths[source] = Some(vec![source]);
        heap.push(IndexState {
            cost: 0.0,
            node: source,
        });

        while let Some(IndexState { cost, node }) = heap.pop() {
            if cost > distances[node] + FLOAT_TOLERANCE {
                continue;
            }
            remaining.remove(&node);
            if remaining.is_empty() {
                break;
            }
            let Some(current_path) = paths[node].clone() else {
                continue;
            };
            for (neighbor, weight) in &weighted_neighbors[node] {
                let next_cost = cost + *weight;
                let mut next_path = current_path.clone();
                next_path.push(*neighbor);
                let should_update = if next_cost + FLOAT_TOLERANCE < distances[*neighbor] {
                    true
                } else if (next_cost - distances[*neighbor]).abs() <= FLOAT_TOLERANCE {
                    match &paths[*neighbor] {
                        None => true,
                        Some(existing_path) => next_path < *existing_path,
                    }
                } else {
                    false
                };
                if should_update {
                    distances[*neighbor] = next_cost;
                    paths[*neighbor] = Some(next_path);
                    heap.push(IndexState {
                        cost: next_cost,
                        node: *neighbor,
                    });
                }
            }
        }

        targets
            .iter()
            .filter_map(|target| {
                let path = paths[*target].clone()?;
                Some((*target, (distances[*target], path)))
            })
            .collect()
    }
}

struct IndexDisjointSet {
    parent: Vec<usize>,
    rank: Vec<usize>,
}

impl IndexDisjointSet {
    fn new(size: usize) -> Self {
        Self {
            parent: (0..size).collect(),
            rank: vec![0; size],
        }
    }

    fn find(&mut self, node: usize) -> usize {
        if self.parent[node] != node {
            let root = self.find(self.parent[node]);
            self.parent[node] = root;
        }
        self.parent[node]
    }

    fn union(&mut self, left: usize, right: usize) -> bool {
        let left_root = self.find(left);
        let right_root = self.find(right);
        if left_root == right_root {
            return false;
        }
        let left_rank = self.rank[left_root];
        let right_rank = self.rank[right_root];
        if left_rank < right_rank {
            self.parent[left_root] = right_root;
        } else if left_rank > right_rank {
            self.parent[right_root] = left_root;
        } else {
            self.parent[right_root] = left_root;
            self.rank[left_root] += 1;
        }
        true
    }
}

fn dedupe_preserve_order(values: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut output = Vec::new();
    for value in values {
        if seen.insert(value.clone()) {
            output.push(value);
        }
    }
    output
}

fn reconstruct_path(
    previous: &HashMap<String, String>,
    source: &str,
    target: &str,
) -> Vec<String> {
    let mut path = vec![target.to_string()];
    let mut current = target.to_string();
    while current != source {
        let Some(parent) = previous.get(&current) else {
            break;
        };
        current = parent.clone();
        path.push(current.clone());
    }
    path.reverse();
    path
}

fn canonical_edge(left: &str, right: &str) -> (String, String) {
    if left <= right {
        (left.to_string(), right.to_string())
    } else {
        (right.to_string(), left.to_string())
    }
}

fn remove_ordered_neighbor(
    order_map: &mut HashMap<String, Vec<String>>,
    owner: &str,
    neighbor: &str,
) {
    if let Some(order) = order_map.get_mut(owner) {
        order.retain(|entry| entry != neighbor);
    }
}
