use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;

fn get_binary_path() -> PathBuf {
    // Find the compiled binary in either debug or release
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let debug_path = Path::new(manifest_dir).join("target/debug/preql-import-resolver");
    let release_path = Path::new(manifest_dir).join("target/release/preql-import-resolver");

    #[cfg(target_os = "windows")]
    {
        let debug_exe = debug_path.with_extension("exe");
        let release_exe = release_path.with_extension("exe");
        if release_exe.exists() {
            return release_exe;
        }
        debug_exe
    }

    #[cfg(not(target_os = "windows"))]
    {
        if release_path.exists() {
            return release_path;
        }
        debug_path
    }
}

fn create_test_file(dir: &Path, name: &str, content: &str) -> PathBuf {
    let path = dir.join(name);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(&path, content).unwrap();
    path
}

#[test]
fn test_parse_simple_file() {
    let temp = TempDir::new().unwrap();
    let root = temp.path();

    let test_file = create_test_file(
        root,
        "test.preql",
        "import models.customer;\nimport models.orders as ord;",
    );

    let output = Command::new(get_binary_path())
        .arg("parse")
        .arg(&test_file)
        .arg("--format")
        .arg("json")
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {:?}", output);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("models.customer"));
    assert!(stdout.contains("models.orders"));
    assert!(stdout.contains("\"alias\":\"ord\""));
}

#[test]
fn test_parse_with_datasource() {
    let temp = TempDir::new().unwrap();
    let root = temp.path();

    let test_file = create_test_file(
        root,
        "test.preql",
        r#"
        datasource orders (
            id: key,
            amount: metric
        )
        address db.orders;
        "#,
    );

    let output = Command::new(get_binary_path())
        .arg("parse")
        .arg(&test_file)
        .arg("--format")
        .arg("json")
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {:?}", output);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"datasources\""));
    assert!(stdout.contains("\"name\":\"orders\""));
}

#[test]
fn test_parse_with_persist() {
    let temp = TempDir::new().unwrap();
    let root = temp.path();

    let test_file = create_test_file(
        root,
        "test.preql",
        "persist orders;\nappend customers where status = 'active';",
    );

    let output = Command::new(get_binary_path())
        .arg("parse")
        .arg(&test_file)
        .arg("--format")
        .arg("json")
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {:?}", output);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"persists\""));
    assert!(stdout.contains("\"target_datasource\":\"orders\""));
    assert!(stdout.contains("\"target_datasource\":\"customers\""));
    assert!(stdout.contains("\"mode\":\"persist\""));
    assert!(stdout.contains("\"mode\":\"append\""));
}

#[test]
fn test_parse_directory() {
    let temp = TempDir::new().unwrap();
    let root = temp.path();

    create_test_file(root, "a.preql", "import b;");
    create_test_file(root, "b.preql", "// no imports");
    create_test_file(root, "c.preql", "import a;");

    let output = Command::new(get_binary_path())
        .arg("parse")
        .arg(root)
        .arg("--format")
        .arg("json")
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {:?}", output);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("a.preql"));
    assert!(stdout.contains("b.preql"));
    assert!(stdout.contains("c.preql"));
}

#[test]
fn test_resolve_simple_dependency() {
    let temp = TempDir::new().unwrap();
    let root = temp.path();

    create_test_file(root, "a.preql", "import b;");
    create_test_file(root, "b.preql", "// no imports");

    let a_path = root.join("a.preql");

    let output = Command::new(get_binary_path())
        .arg("resolve")
        .arg(&a_path)
        .arg("--format")
        .arg("json")
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {:?}", output);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"order\""));
    assert!(stdout.contains("b.preql"));
    assert!(stdout.contains("a.preql"));
}

#[test]
fn test_resolve_with_datasources() {
    let temp = TempDir::new().unwrap();
    let root = temp.path();

    create_test_file(
        root,
        "base.preql",
        r#"
        datasource orders (
            id,
            amount
        )
        address db.orders;
        "#,
    );
    create_test_file(root, "updater.preql", "persist orders;");
    create_test_file(root, "consumer.preql", "import base;");
    create_test_file(root, "main.preql", "import updater;\nimport consumer;");

    let main_path = root.join("main.preql");

    let output = Command::new(get_binary_path())
        .arg("resolve")
        .arg(&main_path)
        .arg("--format")
        .arg("json")
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {:?}", output);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"datasource_declarations\""));
    assert!(stdout.contains("\"datasource_updaters\""));
}

#[test]
fn test_datasources_command() {
    let temp = TempDir::new().unwrap();
    let root = temp.path();

    create_test_file(
        root,
        "models.preql",
        r#"
        datasource customers (
            id: key
        )
        address db.customers;

        datasource orders (
            id: key,
            customer_id
        )
        address db.orders;
        "#,
    );
    create_test_file(root, "updater.preql", "persist orders;");

    let output = Command::new(get_binary_path())
        .arg("datasources")
        .arg(root)
        .arg("--format")
        .arg("json")
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {:?}", output);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"declarations\""));
    assert!(stdout.contains("\"updaters\""));
    assert!(stdout.contains("customers"));
    assert!(stdout.contains("orders"));
}

#[test]
fn test_pretty_format() {
    let temp = TempDir::new().unwrap();
    let root = temp.path();

    let test_file = create_test_file(root, "test.preql", "import models.customer;");

    let output = Command::new(get_binary_path())
        .arg("parse")
        .arg(&test_file)
        .arg("--format")
        .arg("pretty")
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {:?}", output);

    let stdout = String::from_utf8_lossy(&output.stdout);
    // Pretty format should have indentation
    assert!(stdout.contains("  ") || stdout.contains("\n"));
}

#[test]
fn test_direct_only_flag() {
    let temp = TempDir::new().unwrap();
    let root = temp.path();

    create_test_file(root, "a.preql", "import b;");
    create_test_file(root, "b.preql", "import c;");
    create_test_file(root, "c.preql", "// no imports");

    let a_path = root.join("a.preql");

    let output = Command::new(get_binary_path())
        .arg("parse")
        .arg(&a_path)
        .arg("--direct-only")
        .arg("--format")
        .arg("json")
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {:?}", output);

    let stdout = String::from_utf8_lossy(&output.stdout);
    // With --direct-only, should not have resolved_dependencies
    assert!(!stdout.contains("\"resolved_dependencies\"") || stdout.contains("\"resolved_dependencies\":null"));
}

#[test]
fn test_invalid_file() {
    let temp = TempDir::new().unwrap();
    let root = temp.path();
    let nonexistent = root.join("nonexistent.preql");

    let output = Command::new(get_binary_path())
        .arg("parse")
        .arg(&nonexistent)
        .arg("--format")
        .arg("json")
        .output()
        .expect("Failed to execute command");

    assert!(!output.status.success(), "Command should fail for nonexistent file");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("error") || stderr.contains("Error"));
}

#[test]
fn test_recursive_directory_parse() {
    let temp = TempDir::new().unwrap();
    let root = temp.path();

    // Create nested structure
    create_test_file(root, "top.preql", "import nested.a;");
    create_test_file(root, "nested/a.preql", "import b;");
    create_test_file(root, "nested/b.preql", "// no imports");

    let output = Command::new(get_binary_path())
        .arg("parse")
        .arg(root)
        .arg("--recursive")
        .arg("--format")
        .arg("json")
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {:?}", output);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("top.preql"));
    assert!(stdout.contains("a.preql"));
    assert!(stdout.contains("b.preql"));
}

#[test]
fn test_order_only_flag() {
    let temp = TempDir::new().unwrap();
    let root = temp.path();

    create_test_file(root, "a.preql", "import b;");
    create_test_file(root, "b.preql", "// no imports");

    let a_path = root.join("a.preql");

    let output = Command::new(get_binary_path())
        .arg("resolve")
        .arg(&a_path)
        .arg("--order-only")
        .arg("--format")
        .arg("json")
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed: {:?}", output);

    let stdout = String::from_utf8_lossy(&output.stdout);
    // With --order-only, should just be an array of paths
    assert!(stdout.starts_with("["));
    assert!(stdout.ends_with("]\n") || stdout.ends_with("]"));
}
