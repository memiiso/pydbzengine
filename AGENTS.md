# Agent Directives & Repository Standards: pydbzengine

This document defines core guidelines, engineering standards, and quality requirements for AI agents and developers working in `pydbzengine`.

---

## 1. Strict & Simple Architecture (KISS)

1. **Keep it Strict & Simple**: Prioritize clear, direct, and linear code over clever, generic, or highly abstract designs. Avoid unnecessary abstraction layers, indirection, and premature generalization. Logic belongs in the component owning the underlying state.
2. **Concrete over Speculative ABCs**: Favor concrete, decoupled classes. Use `abc.ABC` only when genuine polymorphic contracts or pluggable extensions truly exist (e.g., change handlers). Do not introduce abstract base classes or interfaces for single implementations.
3. **Locality & Boundaries**: Respect module boundaries (`__init__.py`). Handlers depend on engine contracts, never the inverse. The core engine must never depend on optional target sinks (`pyiceberg`, `dlt`, `airflow`). No wildcard imports (`from ... import *`).
4. **Strict Type Safety**: Complete type hints across all signatures. Prefer concrete types over `Any`. Access typed attributes directly (`obj.attr`); never use dynamic reflection (`getattr`/`hasattr`) on dataclasses or domain models. Dynamic access is restricted strictly to JVM interop boundaries (JPype).
5. **Modern Python**: Target Python 3.10+ (use `|` union syntax, `match`, `@cached_property`). Direct and explicit imports. Lazy imports are reserved exclusively for heavy optional integrations (`pyiceberg`, `dlt`, `airflow`).
6. **Clean Logging & Purposeful Docs**: No `print()`; always use `pydbzengine.logger` or standard logging. Write docstrings only when necessary to explain non-obvious *why* and domain intent. Ban obvious code-mechanic summaries and AI/tracking tags.

---

## 2. Eliminate Vibe Code & Defensive Anti-Patterns

1. **Ban Vibe Code**:
   - Eliminate single-consumer helper functions that add indirection without reusability or architectural value—inline them.
   - Eliminate redundant boolean conditions, tautological checks, and unnecessary intermediate variables that only exist to be passed or returned once.
   - Eliminate cosmetic wrappers, dead parameters, and vestigial methods kept "just in case".
   - Address root causes directly; never mask bugs with superficial patches.
2. **Ban Defensive Coding & Silent Fallbacks**:
   - **No Hidden Fallbacks or Magical Logic**: Ban hidden fallback cascades, multi-tier fallback chains, and heuristic guesswork. Do not attempt to guess caller intent or silently recover from invalid state.
   - **No Exception Swallowing**: Never swallow exceptions (`except Exception: pass`, broad catches, or broad `contextlib.suppress`). Never return dummy sentinels (`None`, `{}`, `False`) on failure.
   - **No Just-in-Case Optionality**: If a parameter or dependency is required, declare it mandatory (`p: T`). Never default to `None` or `Optional` out of defensive habits.
   - **No Symptom-Masking**: Ban expressions like `x or {}` and defensive `try...except` blocks that compensate for broken upstream contracts.
3. **Fail-Fast on Unexpected State**:
   - Be simple, strict, and explicit: if an input, format, schema, configuration, or runtime event is invalid or unexpected, fail fast and raise a descriptive domain exception immediately.
   - Preserve error causality with `raise ... from e`.

---

## 3. Modification & Quality Checklist

Before modifying code, confirm:
1. **Finding Worthiness**: Address only empirical bugs, concrete type errors, or clear architectural boundary violations. Reject cosmetic churn, subjective style rewrites, and speculative refactoring. Verify issues statically or with reproduction before touching logic.
2. **Minimal & Surgical Diffs**: Keep diffs focused, minimal, and surgical. Never churn untouched lines, reformat clean code, or bundle unrelated edits. Litmus test: if the change is reverted, does correctness, behavior, or type safety actually degrade?
3. **No Compatibility Shims**: Ban legacy function/class aliases, forwarding wrappers, and dual-path parameter fallbacks. Modify contracts cleanly, migrate all call sites in the same change, and delete obsolete code immediately.

---

## 4. Testing Standards

1. **Production Precedence**: Production code is the source of truth. NEVER weaken production contracts or modify production logic solely to make tests easier to write.
2. **Realistic Payloads over Mock Hierarchies**: For unit tests (schema readers, decoders, event models, configs), test pure logic with realistic Debezium event structures (`tests/support/mock_events.py`) without mocking internal methods. Use `testcontainers` for real end-to-end integration tests (PostgreSQL, MinIO). Ban fragile, deep monkeypatching mock trees.
3. **Behavioral Assertions**: Test observable behavior, data transformations, and state transitions, not implementation trivia. Name tests behaviorally: `test_<feature>_<scenario>`.

---

## 5. Verification Commands

Run before submitting changes:
- **Lint**: `ruff check .` (or `uv run ruff check .`)
- **Type Check**: `mypy pydbzengine` (or `uv run mypy pydbzengine`)
- **Test**: `pytest tests/unit` (unit tests) or `pytest` (or `uv run pytest`)
