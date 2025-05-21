def default_variants():
    return [
        ("prefixed", "prefix_key_shape()"),
        ("uniform", "default_key_shape()"),
    ]

def variants_with_hash_indexed():
    return [
        ("prefixed", "prefix_key_shape()"),
        ("uniform", "default_key_shape()"),
        ("hash_indexed", "hashed_index_key_shape()"),
    ]

def two_key_spaces_variants():
    return [
        ("uniform", "uniform_two_key_spaces()"),
        ("prefix", "prefix_two_key_spaces()"),
    ]

def print_test(name, variants):
    for (variant_name, variant_arg) in variants:
        print("#[test]")
        print("fn " + name + "_" + variant_name + "() {")
        print("    " + name + "(" + variant_arg + ")")
        print("}")
        print()


print("// ! Use generate.sh to generate this file")
print()
print("#[path = \"db_tests.rs\"]")
print("mod db_tests;")
print("use db_tests::*;")
print()
print_test("db_test", variants_with_hash_indexed())
print_test("test_iterator", default_variants())
print_test("test_remove", variants_with_hash_indexed())
print_test("test_multiple_index_formats", two_key_spaces_variants())