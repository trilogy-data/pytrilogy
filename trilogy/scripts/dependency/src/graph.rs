use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};

const FLOAT_TOLERANCE: f64 = 1e-9;

type NodeId = usize;

#[derive(Clone, Default)]
pub struct GraphCore {
    directed: bool,
    node_to_id: HashMap<String, NodeId>,
    id_to_node: Vec<String>,
    node_order: Vec<NodeId>,
    succ: Vec<HashSet<NodeId>>,
    pred: Vec<HashSet<NodeId>>,
    succ_order: Vec<Vec<NodeId>>,
    pred_order: Vec<Vec<NodeId>>,
}

#[derive(Copy, Clone)]
struct IndexState {
    cost: f64,
    node: NodeId,
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
            node_to_id: HashMap::new(),
            id_to_node: Vec::new(),
            node_order: Vec::new(),
            succ: Vec::new(),
            pred: Vec::new(),
            succ_order: Vec::new(),
            pred_order: Vec::new(),
        }
    }

    pub fn directed(&self) -> bool {
        self.directed
    }

    pub fn add_node(&mut self, node: &str) {
        if self.has_node(node) {
            return;
        }
        let owned = node.to_string();
        let id = self.id_to_node.len();
        self.node_to_id.insert(owned.clone(), id);
        self.id_to_node.push(owned);
        self.node_order.push(id);
        self.succ.push(HashSet::new());
        self.pred.push(HashSet::new());
        self.succ_order.push(Vec::new());
        self.pred_order.push(Vec::new());
    }

    pub fn has_node(&self, node: &str) -> bool {
        self.node_to_id.contains_key(node)
    }

    pub fn add_edge(&mut self, left: &str, right: &str) {
        self.add_node(left);
        self.add_node(right);
        let left_id = self
            .node_to_id
            .get(left)
            .copied()
            .expect("left node should exist");
        let right_id = self
            .node_to_id
            .get(right)
            .copied()
            .expect("right node should exist");
        self.insert_edge_ids(left_id, right_id);
    }

    pub fn has_edge(&self, left: &str, right: &str) -> bool {
        let Some(left_id) = self.node_to_id.get(left).copied() else {
            return false;
        };
        let Some(right_id) = self.node_to_id.get(right).copied() else {
            return false;
        };
        self.succ
            .get(left_id)
            .map(|neighbors| neighbors.contains(&right_id))
            .unwrap_or(false)
    }

    pub fn remove_edge(&mut self, left: &str, right: &str) {
        let Some(left_id) = self.node_to_id.get(left).copied() else {
            return;
        };
        let Some(right_id) = self.node_to_id.get(right).copied() else {
            return;
        };
        self.remove_edge_ids(left_id, right_id);
    }

    pub fn remove_edges(&mut self, edges: Vec<(String, String)>) {
        for (left, right) in edges {
            self.remove_edge(&left, &right);
        }
    }

    pub fn remove_node(&mut self, node: &str) {
        let Some(node_id) = self.node_to_id.remove(node) else {
            return;
        };
        let outgoing = self.succ_order[node_id].clone();
        let incoming = self.pred_order[node_id].clone();

        for neighbor in outgoing {
            if self.pred[neighbor].remove(&node_id) {
                remove_ordered_neighbor(&mut self.pred_order[neighbor], node_id);
            }
            if !self.directed && self.succ[neighbor].remove(&node_id) {
                remove_ordered_neighbor(&mut self.succ_order[neighbor], node_id);
            }
        }

        for neighbor in incoming {
            if self.succ[neighbor].remove(&node_id) {
                remove_ordered_neighbor(&mut self.succ_order[neighbor], node_id);
            }
            if !self.directed && self.pred[neighbor].remove(&node_id) {
                remove_ordered_neighbor(&mut self.pred_order[neighbor], node_id);
            }
        }

        self.node_order.retain(|existing| *existing != node_id);
        self.succ[node_id].clear();
        self.pred[node_id].clear();
        self.succ_order[node_id].clear();
        self.pred_order[node_id].clear();
    }

    pub fn remove_nodes(&mut self, nodes: Vec<String>) {
        let removed = nodes
            .into_iter()
            .filter_map(|node| self.node_to_id.get(&node).copied())
            .collect::<HashSet<_>>();
        if removed.is_empty() {
            return;
        }
        if removed.len() <= 8 || removed.len() * 8 < self.node_order.len() {
            let names = removed
                .iter()
                .map(|id| self.id_to_node[*id].clone())
                .collect::<Vec<_>>();
            for name in names {
                self.remove_node(&name);
            }
            return;
        }
        for node_id in &removed {
            self.node_to_id.remove(&self.id_to_node[*node_id]);
            self.succ[*node_id].clear();
            self.pred[*node_id].clear();
            self.succ_order[*node_id].clear();
            self.pred_order[*node_id].clear();
        }
        self.node_order.retain(|node_id| !removed.contains(node_id));
        for node_id in &self.node_order {
            self.succ[*node_id].retain(|neighbor| !removed.contains(neighbor));
            self.pred[*node_id].retain(|neighbor| !removed.contains(neighbor));
            self.succ_order[*node_id].retain(|neighbor| !removed.contains(neighbor));
            self.pred_order[*node_id].retain(|neighbor| !removed.contains(neighbor));
        }
    }

    pub fn nodes(&self) -> Vec<String> {
        self.node_order
            .iter()
            .map(|node_id| self.id_to_node[*node_id].clone())
            .collect()
    }

    pub fn edges(&self) -> Vec<(String, String)> {
        let mut edges = Vec::with_capacity(
            self.node_order
                .iter()
                .map(|node_id| self.succ_order[*node_id].len())
                .sum(),
        );
        let mut seen = HashSet::new();
        for left_id in &self.node_order {
            for right_id in &self.succ_order[*left_id] {
                if self.directed || seen.insert(canonical_id_edge(*left_id, *right_id)) {
                    edges.push((
                        self.id_to_node[*left_id].clone(),
                        self.id_to_node[*right_id].clone(),
                    ));
                }
            }
        }
        edges
    }

    pub fn neighbors(&self, node: &str) -> Vec<String> {
        self.neighbor_names(self.node_to_id.get(node).copied(), false)
    }

    pub fn predecessors(&self, node: &str) -> Vec<String> {
        self.neighbor_names(self.node_to_id.get(node).copied(), true)
    }

    pub fn successors(&self, node: &str) -> Vec<String> {
        self.neighbors(node)
    }

    pub fn all_neighbors(&self, node: &str) -> Vec<String> {
        let Some(node_id) = self.node_to_id.get(node).copied() else {
            return Vec::new();
        };
        let mut seen = HashSet::new();
        let mut output = Vec::new();
        for neighbor in &self.pred_order[node_id] {
            if seen.insert(*neighbor) {
                output.push(self.id_to_node[*neighbor].clone());
            }
        }
        for neighbor in &self.succ_order[node_id] {
            if seen.insert(*neighbor) {
                output.push(self.id_to_node[*neighbor].clone());
            }
        }
        output
    }

    pub fn in_degree(&self, node: &str) -> usize {
        self.node_to_id
            .get(node)
            .map(|node_id| self.pred[*node_id].len())
            .unwrap_or(0)
    }

    pub fn out_degree(&self, node: &str) -> usize {
        self.node_to_id
            .get(node)
            .map(|node_id| self.succ[*node_id].len())
            .unwrap_or(0)
    }

    pub fn clone_graph(&self) -> Self {
        self.clone()
    }

    pub fn induced_subgraph(&self, nodes: Vec<String>) -> Self {
        let keep = nodes
            .into_iter()
            .filter_map(|node| self.node_to_id.get(&node).copied())
            .collect::<HashSet<_>>();
        let ordered_keep = self
            .node_order
            .iter()
            .copied()
            .filter(|node_id| keep.contains(node_id))
            .collect::<Vec<_>>();
        let mut graph = Self::new(self.directed);
        let mut id_map = HashMap::new();
        for old_id in &ordered_keep {
            let name = &self.id_to_node[*old_id];
            graph.add_node(name);
            id_map.insert(
                *old_id,
                graph
                    .node_to_id
                    .get(name)
                    .copied()
                    .expect("new node should exist"),
            );
        }
        for old_id in &ordered_keep {
            let new_id = id_map[old_id];
            graph.succ[new_id] = self.succ[*old_id]
                .iter()
                .filter_map(|neighbor| id_map.get(neighbor).copied())
                .collect();
            graph.pred[new_id] = self.pred[*old_id]
                .iter()
                .filter_map(|neighbor| id_map.get(neighbor).copied())
                .collect();
            graph.succ_order[new_id] = self.succ_order[*old_id]
                .iter()
                .filter_map(|neighbor| id_map.get(neighbor).copied())
                .collect();
            graph.pred_order[new_id] = self.pred_order[*old_id]
                .iter()
                .filter_map(|neighbor| id_map.get(neighbor).copied())
                .collect();
        }
        graph
    }

    pub fn to_undirected_graph(&self) -> Self {
        if !self.directed {
            return self.clone();
        }
        let mut graph = Self::new(false);
        let mut id_map = HashMap::new();
        for node_id in &self.node_order {
            let name = &self.id_to_node[*node_id];
            graph.add_node(name);
            id_map.insert(
                *node_id,
                graph
                    .node_to_id
                    .get(name)
                    .copied()
                    .expect("new node should exist"),
            );
        }
        for left_id in &self.node_order {
            let new_left = id_map[left_id];
            for right_id in &self.succ_order[*left_id] {
                let new_right = id_map[right_id];
                graph.insert_edge_ids(new_left, new_right);
            }
        }
        graph
    }

    pub fn connected_components(&self) -> Vec<Vec<String>> {
        let mut seen = vec![false; self.id_to_node.len()];
        let mut components = Vec::new();

        for node_id in &self.node_order {
            if seen[*node_id] {
                continue;
            }
            let mut queue = VecDeque::from([*node_id]);
            let mut component = Vec::new();
            seen[*node_id] = true;
            while let Some(current) = queue.pop_front() {
                component.push(self.id_to_node[current].clone());
                for neighbor in &self.pred_order[current] {
                    if !seen[*neighbor] {
                        seen[*neighbor] = true;
                        queue.push_back(*neighbor);
                    }
                }
                for neighbor in &self.succ_order[current] {
                    if !seen[*neighbor] {
                        seen[*neighbor] = true;
                        queue.push_back(*neighbor);
                    }
                }
            }
            components.push(component);
        }
        components
    }

    pub fn is_weakly_connected(&self) -> bool {
        let node_count = self.node_order.len();
        if node_count <= 1 {
            return true;
        }
        self.connected_components().len() == 1
    }

    pub fn topological_sort(&self) -> Result<Vec<String>, String> {
        let mut indegree = vec![0usize; self.id_to_node.len()];
        for node_id in &self.node_order {
            indegree[*node_id] = self.pred[*node_id].len();
        }

        let mut ready = VecDeque::new();
        for node_id in &self.node_order {
            if indegree[*node_id] == 0 {
                ready.push_back(*node_id);
            }
        }

        let mut output = Vec::new();
        while let Some(node_id) = ready.pop_front() {
            output.push(self.id_to_node[node_id].clone());
            for neighbor in &self.succ_order[node_id] {
                indegree[*neighbor] -= 1;
                if indegree[*neighbor] == 0 {
                    ready.push_back(*neighbor);
                }
            }
        }

        if output.len() != self.node_order.len() {
            return Err("Graph contains a cycle".to_string());
        }
        Ok(output)
    }

    pub fn shortest_path(&self, source: &str, target: &str) -> Option<Vec<String>> {
        let source_id = self.node_to_id.get(source).copied()?;
        let target_id = self.node_to_id.get(target).copied()?;
        if source_id == target_id {
            return Some(vec![source.to_string()]);
        }

        let mut queue = VecDeque::from([source_id]);
        let mut visited = vec![false; self.id_to_node.len()];
        let mut previous = vec![None; self.id_to_node.len()];
        visited[source_id] = true;

        while let Some(current) = queue.pop_front() {
            for neighbor in &self.succ_order[current] {
                if visited[*neighbor] {
                    continue;
                }
                visited[*neighbor] = true;
                previous[*neighbor] = Some(current);
                if *neighbor == target_id {
                    return Some(self.reconstruct_id_path(&previous, source_id, target_id));
                }
                queue.push_back(*neighbor);
            }
        }

        None
    }

    pub fn shortest_path_length(&self, source: &str, target: &str) -> Option<usize> {
        self.shortest_path(source, target)
            .map(|path| path.len().saturating_sub(1))
    }

    pub fn ego_graph_nodes(&self, center: &str, radius: usize) -> Vec<String> {
        let Some(center_id) = self.node_to_id.get(center).copied() else {
            return Vec::new();
        };

        let mut queue = VecDeque::from([(center_id, 0usize)]);
        let mut visited = vec![false; self.id_to_node.len()];
        visited[center_id] = true;

        while let Some((current, depth)) = queue.pop_front() {
            if depth >= radius {
                continue;
            }
            for neighbor in &self.succ_order[current] {
                if !visited[*neighbor] {
                    visited[*neighbor] = true;
                    queue.push_back((*neighbor, depth + 1));
                }
            }
        }

        self.node_order
            .iter()
            .filter(|node_id| visited[**node_id])
            .map(|node_id| self.id_to_node[*node_id].clone())
            .collect()
    }

    pub fn multi_source_dijkstra_path(
        &self,
        sources: Vec<String>,
        weights: Vec<(String, String, f64)>,
    ) -> Result<Vec<(String, Vec<String>)>, String> {
        let source_ids = dedupe_preserve_order(
            sources
                .into_iter()
                .map(|source| {
                    self.node_to_id
                        .get(&source)
                        .copied()
                        .ok_or_else(|| format!("Node not found: {source}"))
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
        if source_ids.is_empty() {
            return Ok(Vec::new());
        }

        let weighted_neighbors = self.build_weighted_neighbors(weights)?;
        let ranks = self.lex_ranks();
        let mut heap = BinaryHeap::new();
        let mut distances = vec![f64::INFINITY; self.id_to_node.len()];
        let mut paths: Vec<Option<Vec<NodeId>>> = vec![None; self.id_to_node.len()];

        for source_id in &source_ids {
            distances[*source_id] = 0.0;
            paths[*source_id] = Some(vec![*source_id]);
            heap.push(IndexState {
                cost: 0.0,
                node: *source_id,
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
                        Some(existing_path) => path_less(&next_path, existing_path, &ranks),
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
        for node_id in &self.node_order {
            let Some(path) = &paths[*node_id] else {
                continue;
            };
            output.push((
                self.id_to_node[*node_id].clone(),
                path.iter()
                    .map(|path_id| self.id_to_node[*path_id].clone())
                    .collect(),
            ));
        }
        Ok(output)
    }

    pub fn steiner_tree_nodes(
        &self,
        terminals: Vec<String>,
        weights: Vec<(String, String, f64)>,
    ) -> Result<Vec<String>, String> {
        let terminal_ids = dedupe_preserve_order(
            terminals
                .into_iter()
                .map(|terminal| {
                    self.node_to_id
                        .get(&terminal)
                        .copied()
                        .ok_or_else(|| format!("Node not found: {terminal}"))
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
        if terminal_ids.is_empty() {
            return Ok(Vec::new());
        }
        if terminal_ids.len() == 1 {
            return Ok(vec![self.id_to_node[terminal_ids[0]].clone()]);
        }

        let weighted_neighbors = self.build_weighted_neighbors(weights)?;
        let ranks = self.lex_ranks();
        let mut metric_edges: Vec<(f64, NodeId, NodeId, Vec<NodeId>)> = Vec::new();

        for index in 0..terminal_ids.len() {
            let left = terminal_ids[index];
            let targets = &terminal_ids[index + 1..];
            let shortest_paths = self.weighted_shortest_paths_to_target_indices(
                left,
                targets,
                &weighted_neighbors,
                &ranks,
            );
            for right in targets {
                let Some((distance, path)) = shortest_paths.get(right) else {
                    return Err(format!(
                        "No path between {} and {}",
                        self.id_to_node[left], self.id_to_node[*right]
                    ));
                };
                metric_edges.push((*distance, left, *right, path.clone()));
            }
        }

        metric_edges.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| ranks[left.1].cmp(&ranks[right.1]))
                .then_with(|| ranks[left.2].cmp(&ranks[right.2]))
        });

        let mut metric_dsu = IndexDisjointSet::new(self.id_to_node.len());
        let mut expanded_nodes = terminal_ids.iter().copied().collect::<HashSet<_>>();
        for (_distance, left, right, path) in metric_edges {
            if metric_dsu.union(left, right) {
                expanded_nodes.extend(path);
            }
        }

        Ok(self.finalize_steiner_tree(
            terminal_ids,
            expanded_nodes,
            &weighted_neighbors,
        ))
    }

    fn insert_edge_ids(&mut self, left_id: NodeId, right_id: NodeId) {
        if self.succ[left_id].insert(right_id) {
            self.succ_order[left_id].push(right_id);
        }
        if self.pred[right_id].insert(left_id) {
            self.pred_order[right_id].push(left_id);
        }
        if !self.directed {
            if self.succ[right_id].insert(left_id) {
                self.succ_order[right_id].push(left_id);
            }
            if self.pred[left_id].insert(right_id) {
                self.pred_order[left_id].push(right_id);
            }
        }
    }

    fn remove_edge_ids(&mut self, left_id: NodeId, right_id: NodeId) {
        if self.succ[left_id].remove(&right_id) {
            remove_ordered_neighbor(&mut self.succ_order[left_id], right_id);
        }
        if self.pred[right_id].remove(&left_id) {
            remove_ordered_neighbor(&mut self.pred_order[right_id], left_id);
        }
        if !self.directed {
            if self.succ[right_id].remove(&left_id) {
                remove_ordered_neighbor(&mut self.succ_order[right_id], left_id);
            }
            if self.pred[left_id].remove(&right_id) {
                remove_ordered_neighbor(&mut self.pred_order[left_id], right_id);
            }
        }
    }

    fn neighbor_names(&self, node_id: Option<NodeId>, reverse: bool) -> Vec<String> {
        let Some(node_id) = node_id else {
            return Vec::new();
        };
        let neighbors = if reverse {
            &self.pred_order[node_id]
        } else {
            &self.succ_order[node_id]
        };
        neighbors
            .iter()
            .map(|neighbor| self.id_to_node[*neighbor].clone())
            .collect()
    }

    fn reconstruct_id_path(
        &self,
        previous: &[Option<NodeId>],
        source: NodeId,
        target: NodeId,
    ) -> Vec<String> {
        let mut path = vec![self.id_to_node[target].clone()];
        let mut current = target;
        while current != source {
            let Some(parent) = previous[current] else {
                break;
            };
            current = parent;
            path.push(self.id_to_node[current].clone());
        }
        path.reverse();
        path
    }

    fn lex_ranks(&self) -> Vec<usize> {
        let mut ranked = self.node_order.clone();
        ranked.sort_by(|left, right| self.id_to_node[*left].cmp(&self.id_to_node[*right]));
        let mut ranks = vec![usize::MAX; self.id_to_node.len()];
        for (rank, node_id) in ranked.into_iter().enumerate() {
            ranks[node_id] = rank;
        }
        ranks
    }

    fn build_weighted_neighbors(
        &self,
        weights: Vec<(String, String, f64)>,
    ) -> Result<Vec<Vec<(NodeId, f64)>>, String> {
        let mut overrides =
            HashMap::with_capacity(weights.len() * if self.directed { 1 } else { 2 });
        for (left, right, weight) in weights {
            let Some(left_id) = self.node_to_id.get(&left).copied() else {
                return Err(format!("Node not found: {left}"));
            };
            let Some(right_id) = self.node_to_id.get(&right).copied() else {
                return Err(format!("Node not found: {right}"));
            };
            overrides.insert((left_id, right_id), weight);
            if !self.directed {
                overrides.insert((right_id, left_id), weight);
            }
        }

        let mut weighted_neighbors = vec![Vec::<(NodeId, f64)>::new(); self.id_to_node.len()];
        for node_id in &self.node_order {
            let mut neighbors = Vec::with_capacity(self.succ_order[*node_id].len());
            for neighbor in &self.succ_order[*node_id] {
                neighbors.push((
                    *neighbor,
                    overrides
                        .get(&(*node_id, *neighbor))
                        .copied()
                        .unwrap_or(1.0),
                ));
            }
            weighted_neighbors[*node_id] = neighbors;
        }
        Ok(weighted_neighbors)
    }

    fn finalize_steiner_tree(
        &self,
        terminals: Vec<NodeId>,
        expanded_nodes: HashSet<NodeId>,
        weighted_neighbors: &[Vec<(NodeId, f64)>],
    ) -> Vec<String> {
        let ordered_expanded_nodes = self
            .node_order
            .iter()
            .copied()
            .filter(|node_id| expanded_nodes.contains(node_id))
            .collect::<Vec<_>>();
        let mut position = vec![usize::MAX; self.id_to_node.len()];
        for (index, node_id) in ordered_expanded_nodes.iter().enumerate() {
            position[*node_id] = index;
        }

        let mut original_edges = Vec::new();
        let mut seen_edges = HashSet::new();
        for left_id in &ordered_expanded_nodes {
            for (right_id, weight) in &weighted_neighbors[*left_id] {
                if position[*right_id] == usize::MAX {
                    continue;
                }
                if self.directed || seen_edges.insert(canonical_id_edge(*left_id, *right_id)) {
                    original_edges.push((*weight, *left_id, *right_id));
                }
            }
        }
        let ranks = self.lex_ranks();
        original_edges.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| ranks[left.1].cmp(&ranks[right.1]))
                .then_with(|| ranks[left.2].cmp(&ranks[right.2]))
        });

        let mut tree = vec![HashSet::<NodeId>::new(); self.id_to_node.len()];
        let mut induced_dsu = IndexDisjointSet::new(self.id_to_node.len());
        for (_weight, left, right) in original_edges {
            if induced_dsu.union(left, right) {
                tree[left].insert(right);
                tree[right].insert(left);
            }
        }

        let terminals_set = terminals.into_iter().collect::<HashSet<_>>();
        let mut removable = ordered_expanded_nodes
            .iter()
            .filter_map(|node_id| {
                if !terminals_set.contains(node_id) && tree[*node_id].len() <= 1 {
                    Some(*node_id)
                } else {
                    None
                }
            })
            .collect::<VecDeque<_>>();
        let mut removed = vec![false; self.id_to_node.len()];
        while let Some(node_id) = removable.pop_front() {
            if removed[node_id] || terminals_set.contains(&node_id) || tree[node_id].len() > 1 {
                continue;
            }
            removed[node_id] = true;
            let neighbors = std::mem::take(&mut tree[node_id]);
            for neighbor in neighbors {
                if tree[neighbor].remove(&node_id)
                    && !terminals_set.contains(&neighbor)
                    && tree[neighbor].len() <= 1
                {
                    removable.push_back(neighbor);
                }
            }
        }

        self.node_order
            .iter()
            .filter(|node_id| {
                expanded_nodes.contains(node_id)
                    && !removed[**node_id]
                    && (!tree[**node_id].is_empty() || terminals_set.contains(node_id))
            })
            .map(|node_id| self.id_to_node[*node_id].clone())
            .collect()
    }

    fn weighted_shortest_paths_to_target_indices(
        &self,
        source: NodeId,
        targets: &[NodeId],
        weighted_neighbors: &[Vec<(NodeId, f64)>],
        ranks: &[usize],
    ) -> HashMap<NodeId, (f64, Vec<NodeId>)> {
        if targets.is_empty() {
            return HashMap::new();
        }
        let mut remaining = targets.iter().copied().collect::<HashSet<_>>();
        let mut heap = BinaryHeap::new();
        let mut distances = vec![f64::INFINITY; self.id_to_node.len()];
        let mut paths: Vec<Option<Vec<NodeId>>> = vec![None; self.id_to_node.len()];

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
                        Some(existing_path) => path_less(&next_path, existing_path, ranks),
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
    parent: Vec<NodeId>,
    rank: Vec<usize>,
}

impl IndexDisjointSet {
    fn new(size: usize) -> Self {
        Self {
            parent: (0..size).collect(),
            rank: vec![0; size],
        }
    }

    fn find(&mut self, node: NodeId) -> NodeId {
        if self.parent[node] != node {
            let root = self.find(self.parent[node]);
            self.parent[node] = root;
        }
        self.parent[node]
    }

    fn union(&mut self, left: NodeId, right: NodeId) -> bool {
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

fn dedupe_preserve_order<T: Eq + std::hash::Hash + Copy>(values: Vec<T>) -> Vec<T> {
    let mut seen = HashSet::new();
    let mut output = Vec::new();
    for value in values {
        if seen.insert(value) {
            output.push(value);
        }
    }
    output
}

fn canonical_id_edge(left: NodeId, right: NodeId) -> (NodeId, NodeId) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

fn remove_ordered_neighbor(order: &mut Vec<NodeId>, neighbor: NodeId) {
    order.retain(|entry| *entry != neighbor);
}

fn path_less(left: &[NodeId], right: &[NodeId], ranks: &[usize]) -> bool {
    for (left_node, right_node) in left.iter().zip(right.iter()) {
        match ranks[*left_node].cmp(&ranks[*right_node]) {
            Ordering::Less => return true,
            Ordering::Greater => return false,
            Ordering::Equal => {}
        }
    }
    left.len() < right.len()
}
