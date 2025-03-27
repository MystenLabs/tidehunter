def default_variants():
    return [("prefixed", "prefix_key_shape()"), ("uniform", "default_key_shape()")]


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
print_test("db_test", default_variants())
print_test("test_iterator", default_variants())
print_test("test_remove", default_variants())
