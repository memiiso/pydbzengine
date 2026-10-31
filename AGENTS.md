# Agent Directives & Repository Standards: pydbzengine

This document defines core guidelines, engineering standards, and quality requirements for AI agents and developers working in `pydbzengine`.

---

## 1. Strict, Simple & Domain-Driven Architecture

1. **Simple, Elegant, Deterministic & Strict**: Keep code simple, strict, deterministic, and elegant. Prioritize clear, direct, and linear execution paths over clever, generic, or overly abstract designs. Given a configuration and input, execution must follow a single deterministic path with zero runtime guesswork.
2. **Complete Domain-Driven Design (DDD) over Arbitrary Size Metrics**:
   - Model the system around rich, cohesive domain entities that faithfully represent real-world concepts.
   - Each entity class must have a well-defined purpose and own its complete domain role, state, and behavior.
   - **Ignore Arbitrary LOC or "God Class" Constraints**: Do not fragment or split a cohesive domain entity across multiple files or helper classes just to reduce Lines of Code (LOC) or satisfy artificial class-size metrics. A complete, self-contained domain representation always takes precedence over arbitrary technical size constraints.
3. **Concrete over Speculative ABCs**: Favor concrete, decoupled classes. Use `abc.ABC` only when genuine polymorphic contracts or pluggable extensions truly exist (e.g., change handlers, data type handlers). Do not introduce abstract base classes or interfaces for single implementations.
4. **Locality & Boundaries**: Respect module boundaries (`__init__.py`). Handlers depend on engine contracts, never the inverse. The core engine must never depend on optional target sinks (`pyiceberg`, `dlt`, `airflow`). No wildcard imports (`from ... import *`).
5. **Strict Type Safety & No Dynamic Attribute Reflection**: Complete type hints across all signatures. Prefer concrete types over `Any`. Access typed attributes directly (`obj.attr`); **never** use defensive dynamic reflection (`getattr`, `hasattr`, `__getattribute__`) on dataclasses, domain entities, or typed models.
6. **Modern Python**: Target Python 3.10+ (use `|` union syntax, `match`, `@cached_property`). Direct and explicit imports. Lazy imports are reserved exclusively for heavy optional integrations (`pyiceberg`, `dlt`, `airflow`).
7. **Clean Logging & Purposeful Docs**: No `print()`; always use `pydbzengine.logger` or standard logging. Write docstrings only when necessary to explain non-obvious *why* and domain intent. Ban obvious code-mechanic summaries and AI/tracking tags.

---

## 2. Eliminate Vibe Code, Defensive Coding & Fallback Logic

1. **Ban Vibe Code**:
   - Keep code simple and elegant—no vibe coding.
   - Eliminate single-consumer helper functions that add indirection without reusability or architectural value—inline them or keep logic as methods on the owning domain entity.
   - Eliminate redundant boolean conditions, tautological checks, and unnecessary intermediate variables that only exist to be passed or returned once.
   - Eliminate cosmetic wrappers, dead parameters, and vestigial methods kept "just in case".
   - Address root causes directly; never mask bugs with superficial patches.
2. **Ban Defensive Coding & Silent Fallbacks**:
   - **No Defensive Coding (`getattr`, `hasattr`, etc.)**: Do not write defensive attribute probes, type-guessing branches, or `x or {}` symptom-masking expressions. Trust and enforce explicit contracts.
   - **No Fallback Logic That Hides Actual Issues**: Ban fallback cascades, multi-tier fallback chains, and heuristic recovery logic that hide real bugs and create unseen, unpredictable runtime behaviors.
   - **No Exception Swallowing**: Never swallow exceptions (`except Exception: pass`, broad catches, or broad `contextlib.suppress`). Never return dummy sentinels (`None`, `{}`, `False`) on failure.
   - **No Just-in-Case Optionality**: If a parameter or dependency is required, declare it mandatory (`p: T`). Never default to `None` or `Optional` out of defensive habits.
3. **Be Deterministic & Fail-Fast on Unexpected State**:
   - Be simple, strict, deterministic, and fail fast: if an input, format, schema, configuration, or runtime event is invalid or unexpected, raise a descriptive domain exception immediately.
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
- **Test**: `python -m unittest discover -s tests/unit` (or `uv run python -m unittest discover -s tests/unit`)
