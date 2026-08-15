mod parser;
mod resolver;
mod directory_resolver;
mod graph;
mod network_search;
mod trilogy_parser;
#[cfg(feature = "python")]
pub mod python_bindings;

// The pest parser over the full Trilogy grammar. Exported because
// `ParseError::PestError` already surfaces `Rule` in its public signature —
// without this a consumer cannot name that type, let alone match on it — and
// because the raw parse tree is the reason non-Python tooling wants the crate.
pub use trilogy_parser::{Rule, TrilogyParser};

pub use parser::{
    parse_file, parse_imports, AddressKind, DatasourceDeclaration, ImportStatement, ParseError,
    ParsedFile, PersistMode, PersistStatement,
};
pub use resolver::{
    DatasourceInfo, DependencyGraph, FileNode, ImportInfo, ImportResolver, PersistInfo,
    ResolveError,
};
pub use directory_resolver::{
    collect_preql_files, process_directory_with_imports, build_edges, DirectoryGraph, Edge,
    EdgeReason, FileInfo,
};
pub use graph::GraphCore;
pub use network_search::{enumerate_covers, CandidateSpec, LimitKind, NetworkSpec};
