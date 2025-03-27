// ! Use generate.sh to generate this file

#[path = "db_tests.rs"]
mod db_tests;
use db_tests::*;

#[test]
fn db_test_prefixed() {
    db_test(prefix_key_shape())
}

#[test]
fn db_test_uniform() {
    db_test(default_key_shape())
}

#[test]
fn test_iterator_prefixed() {
    test_iterator(prefix_key_shape())
}

#[test]
fn test_iterator_uniform() {
    test_iterator(default_key_shape())
}

#[test]
fn test_remove_prefixed() {
    test_remove(prefix_key_shape())
}

#[test]
fn test_remove_uniform() {
    test_remove(default_key_shape())
}
