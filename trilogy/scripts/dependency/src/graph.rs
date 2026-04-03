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
        let removed = nodes
            .into_iter()
            .filter(|node| self.succ.contains_key(node))
            .collect::<HashSet<_>>();
        if removed.is_empty() {
            return;
        }
        if removed.len() <= 8 || removed.len() * 8 < self.node_order.len() {
            for node in removed {
                self.remove_node(&node);
            }
            return;
        }
        self.node_order.retain(|node| !removed.contains(node));
        for node in &removed {
            self.succ.remove(node);
            self.pred.remove(node);
            self.succ_order.remove(node);
            self.pred_order.remove(node);
        }
        for node in &self.node_order {
            if let Some(neighbors) = self.succ.get_mut(node) {
                neighbors.retain(|neighbor| !removed.contains(neighbor));
            }
            if let Some(neighbors) = self.pred.get_mut(node) {
                neighbors.retain(|neighbor| !removed.contains(neighbor));
            }
            if let Some(order) = self.succ_order.get_mut(node) {
                order.retain(|neighbor| !removed.contains(neighbor));
            }
            if let Some(order) = self.pred_order.get_mut(node) {
                order.retain(|neighbor| !removed.contains(neighbor));
            }
        }
    }

    pub fn nodes(&self) -> Vec<String> {
        self.node_order.clone()
    }

    pub fn edges(&self) -> Vec<(String, String)> {
        let mut edges = Vec::with_capacity(
            self.succ_order.values().map(|neighbors| neighbors.len()).sum(),
        );
        let mut seen = HashSet::new();
        for left in &self.node_order {
            if let Some(neighbors) = self.succ_order.get(left) {
                for right in neighbors {
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
        self.succ_order.get(node).cloned().unwrap_or_default()
    }

    pub fn predecessors(&self, node: &str) -> Vec<String> {
        self.pred_order.get(node).cloned().unwrap_or_default()
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
        graph.node_order = self
            .node_order
            .iter()
            .filter(|node| keep.contains(*node))
            .cloned()
            .collect();
        for node in &graph.node_order {
            graph.succ.insert(
                node.clone(),
                self.succ
                    .get(node)
                    .map(|neighbors| {
                        neighbors
                            .iter()
                            .filter(|neighbor| keep.contains(*neighbor))
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default(),
            );
            graph.pred.insert(
                node.clone(),
                self.pred
                    .get(node)
                    .map(|neighbors| {
                        neighbors
                            .iter()
                            .filter(|neighbor| keep.contains(*neighbor))
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default(),
            );
            graph.succ_order.insert(
                node.clone(),
                self.succ_order
                    .get(node)
                    .map(|neighbors| {
                        neighbors
                            .iter()
                            .filter(|neighbor| keep.contains(*neighbor))
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default(),
            );
            graph.pred_order.insert(
                node.clone(),
                self.pred_order
                    .get(node)
                    .map(|neighbors| {
                        neighbors
                            .iter()
                            .filter(|neighbor| keep.contains(*neighbor))
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default(),
            );
        }
        graph
    }

    pub fn to_undirected_graph(&self) -> Self {
        if !self.directed {
            return self.clone();
        }
        let mut graph = Self::new(false);
        graph.node_order = self.node_order.clone();
        for node in &graph.node_order {
            graph.succ.insert(node.clone(), HashSet::new());
            graph.pred.insert(node.clone(), HashSet::new());
            graph.succ_order.insert(node.clone(), Vec::new());
            graph.pred_order.insert(node.clone(), Vec::new());
        }
        for left in &self.node_order {
            let Some(neighbors) = self.succ_order.get(left) else {
                continue;
            };
            for right in neighbors {
                if graph
                    .succ
                    .get_mut(left)
                    .expect("left node exists")
                    .insert(right.clone())
                {
                    graph
                        .succ_order
                        .get_mut(left)
                        .expect("left order exists")
                        .push(right.clone());
                    graph
                        .pred
                        .get_mut(right)
                        .expect("right pred exists")
                        .insert(left.clone());
                    graph
                        .pred_order
                        .get_mut(right)
                        .expect("right pred order exists")
                        .push(left.clone());
                }
                if graph
                    .succ
                    .get_mut(right)
                    .expect("right node exists")
                    .insert(left.clone())
                {
                    graph
                        .succ_order
                        .get_mut(right)
                        .expect("right order exists")
                        .push(left.clone());
                    graph
                        .pred
                        .get_mut(left)
                        .expect("left pred exists")
                        .insert(right.clone());
                    graph
                        .pred_order
                        .get_mut(left)
                        .expect("left pred order exists")
                        .push(right.clone());
                }
            }
        }
        graph
    }

    pub fn connected_components(&self) -> Vec<Vec<String>> {
        let mut seen = HashSet::new();
        let mut components = Vec::new();

        for node in &self.node_order {
            if seen.contains(node) {
                continue;
            }
            let mut queue = VecDeque::from([node.clone()]);
            let mut component = Vec::new();
            seen.insert(node.clone());
            while let Some(current) = queue.pop_front() {
                component.push(current.clone());
                if let Some(neighbors) = self.pred_order.get(&current) {
                    for neighbor in neighbors {
                        if seen.insert(neighbor.clone()) {
                            queue.push_back(neighbor.clone());
                        }
                    }
                }
                if let Some(neighbors) = self.succ_order.get(&current) {
                    for neighbor in neighbors {
                        if seen.insert(neighbor.clone()) {
                            queue.push_back(neighbor.clone());
                        }
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
            if let Some(neighbors) = self.succ_order.get(&node) {
                for neighbor in neighbors {
                    if let Some(entry) = indegree.get_mut(neighbor) {
                        *entry -= 1;
                        if *entry == 0 {
                            ready.push_back(neighbor.clone());
                        }
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
            let Some(neighbors) = self.succ_order.get(&current) else {
                continue;
            };
            for neighbor in neighbors {
                if !visited.insert(neighbor.clone()) {
                    continue;
                }
                previous.insert(neighbor.clone(), current.clone());
                if neighbor == target {
                    return Some(reconstruct_path(&previous, source, target));
                }
                queue.push_back(neighbor.clone());
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
            let Some(neighbors) = self.succ_order.get(&current) else {
                continue;
            };
            for neighbor in neighbors {
                if visited.insert(neighbor.clone()) {
                    queue.push_back((neighbor.clone(), depth + 1));
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
        let lex_nodes = {
            let mut nodes = self.nodes();
            nodes.sort();
            nodes
        };
        let mut node_to_index = HashMap::new();
        for (index, node) in lex_nodes.iter().enumerate() {
            node_to_index.insert(node.clone(), index);
        }
        let mut source_indices = Vec::with_capacity(source_set.len());
        for source in &source_set {
            let Some(index) = node_to_index.get(source).copied() else {
                return Err(format!("Node not found: {source}"));
            };
            source_indices.push(index);
        }

        let weight_map = self.weight_map(weights);
        let mut weighted_neighbors = vec![Vec::<(usize, f64)>::new(); lex_nodes.len()];
        for (index, left) in lex_nodes.iter().enumerate() {
            let Some(neighbors) = self.succ_order.get(left) else {
                continue;
            };
            for right in neighbors {
                let Some(right_index) = node_to_index.get(right).copied() else {
                    continue;
                };
                weighted_neighbors[index]
                    .push((right_index, self.edge_weight(left, right, &weight_map)));
            }
        }

        let mut heap = BinaryHeap::new();
        let mut distances = vec![f64::INFINITY; lex_nodes.len()];
        let mut paths: Vec<Option<Vec<usize>>> = vec![None; lex_nodes.len()];

        for source in source_indices {
            distances[source] = 0.0;
            paths[source] = Some(vec![source]);
            heap.push(IndexState {
                cost: 0.0,
                node: source,
            });
        }

        while let Some(IndexState { cost, node }) = heap.pop() {
            if cost > distances[node] + FLOAT_TOLERANCE {
                continue;
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

        let mut output = Vec::new();
        for node in self.nodes() {
            let Some(&index) = node_to_index.get(&node) else {
                continue;
            };
            let Some(path) = &paths[index] else {
                continue;
            };
            output.push((
                node,
                path.iter().map(|index| lex_nodes[*index].clone()).collect(),
            ));
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

    fn finalize_steiner_tree(
        &self,
        terminals: Vec<String>,
        expanded_nodes: HashSet<String>,
        weight_map: &HashMap<String, HashMap<String, f64>>,
    ) -> Vec<String> {
        let ordered_expanded_nodes = self
            .nodes()
            .into_iter()
            .filter(|node| expanded_nodes.contains(node))
            .collect::<Vec<_>>();
        let mut node_to_index = HashMap::new();
        for (index, node) in ordered_expanded_nodes.iter().enumerate() {
            node_to_index.insert(node.clone(), index);
        }
        let mut original_edges = Vec::new();
        let mut seen_edges = HashSet::new();
        for left in &ordered_expanded_nodes {
            let Some(neighbors) = self.succ_order.get(left) else {
                continue;
            };
            let Some(&left_index) = node_to_index.get(left) else {
                continue;
            };
            for right in neighbors {
                let Some(&right_index) = node_to_index.get(right) else {
                    continue;
                };
                let key = if self.directed || left_index <= right_index {
                    (left_index, right_index)
                } else {
                    (right_index, left_index)
                };
                if self.directed || seen_edges.insert(key) {
                    original_edges.push((
                        self.edge_weight(left, right, &weight_map),
                        left_index,
                        right_index,
                    ));
                }
            }
        }
        original_edges.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| ordered_expanded_nodes[left.1].cmp(&ordered_expanded_nodes[right.1]))
                .then_with(|| ordered_expanded_nodes[left.2].cmp(&ordered_expanded_nodes[right.2]))
        });

        let mut tree = vec![HashSet::<usize>::new(); ordered_expanded_nodes.len()];
        let mut induced_dsu = IndexDisjointSet::new(ordered_expanded_nodes.len());
        for (_weight, left, right) in original_edges {
            if induced_dsu.union(left, right) {
                tree[left].insert(right);
                tree[right].insert(left);
            }
        }

        let terminals_set = terminals.into_iter().collect::<HashSet<_>>();
        let mut removable = ordered_expanded_nodes
            .iter()
            .enumerate()
            .filter_map(|node| {
                let (index, name) = node;
                if !terminals_set.contains(name) && tree[index].len() <= 1 {
                    Some(index)
                } else {
                    None
                }
            })
            .collect::<VecDeque<_>>();
        let mut removed = vec![false; ordered_expanded_nodes.len()];
        while let Some(node_index) = removable.pop_front() {
            if removed[node_index]
                || terminals_set.contains(&ordered_expanded_nodes[node_index])
                || tree[node_index].len() > 1
            {
                continue;
            }
            removed[node_index] = true;
            let neighbors = std::mem::take(&mut tree[node_index]);
            for neighbor in neighbors {
                if tree[neighbor].remove(&node_index)
                    && !terminals_set.contains(&ordered_expanded_nodes[neighbor])
                    && tree[neighbor].len() <= 1
                {
                    removable.push_back(neighbor);
                }
            }
        }

        self.nodes()
            .into_iter()
            .filter(|node| {
                node_to_index
                    .get(node)
                    .copied()
                    .map(|index| {
                        !removed[index]
                            && (!tree[index].is_empty() || terminals_set.contains(node))
                    })
                    .unwrap_or(false)
            })
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
