
            /// Returns the `rustc` SemVer version and additional metadata
            /// like the git short hash and build date.
            pub fn version_meta() -> VersionMeta {
                VersionMeta {
                    semver: Version {
                        major: 1,
                        minor: 90,
                        patch: 0,
                        pre: Prerelease::new("").unwrap(),
                        build: BuildMetadata::new("").unwrap(),
                    },
                    host: "aarch64-apple-darwin".to_owned(),
                    short_version_string: "rustc 1.90.0 (1159e78c4 2025-09-14)".to_owned(),
                    commit_hash: Some("1159e78c4747b02ef996e55082b704c09b970588".to_owned()),
                    commit_date: Some("2025-09-14".to_owned()),
                    build_date: None,
                    channel: Channel::Stable,
                    llvm_version: Some(LlvmVersion{ major: 20, minor: 1 }),
                }
            }
            