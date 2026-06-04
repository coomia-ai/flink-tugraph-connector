# Contributing to flink-tugraph-connector

Thanks for your interest in improving the connector! This document explains how to build, test and
submit changes.

## Development environment

- **JDK 21** (the Gradle toolchain enforces this).
- **Gradle** — use the bundled wrapper (`./gradlew`), no separate install needed.
- **Docker** — only required to run the integration tests.

## Build & test

```bash
./gradlew build        # compile + unit tests + shaded jar
./gradlew test         # unit tests only
./gradlew shadowJar    # the redistributable fat jar
```

Integration tests boot a real TuGraph container and are skipped unless `TUGRAPH_IT=1`:

```bash
TUGRAPH_IT=1 ./gradlew test
```

## Coding guidelines

- **Language:** all code, comments and documentation are written in **English**.
- **Style:** follow the conventions already present in the codebase — small, focused classes,
  Javadoc on public types/methods, immutable value objects, no unnecessary mutability.
- **Tests:** new logic needs unit tests; behaviour that touches TuGraph belongs in a
  `*IT` / `*ITCase` integration test gated on `TUGRAPH_IT`.
- **Public API changes** must be reflected in the README and `CHANGELOG.md`.

## Commit messages

This project uses [Conventional Commits](https://www.conventionalcommits.org/) in English:

```
<type>(<optional scope>): <summary>

[optional body]
```

Common types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`, `chore`, `ci`.
Examples:

```
feat(sink): add time-based flush to TuGraphSinkWriter
fix(cypher): escape embedded backticks in identifiers
docs(readme): document edge.src.key option
```

## Pull requests

1. Fork and create a topic branch (`feat/...`, `fix/...`).
2. Keep changes focused; run `./gradlew build` before pushing.
3. Open a PR describing the motivation and the testing you performed.
4. By contributing you agree your work is licensed under the project's Apache 2.0 license.
