<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Design: Gravitino Tags as IRC `labels`

| Field   | Value                                                                 |
| ------- | --------------------------------------------------------------------- |
| Status  | Draft — Proof of Concept                                              |
| Authors | @laskoviymishka                                                       |
| Module  | `iceberg/iceberg-rest-server`, `iceberg/iceberg-common`               |
| Related | Iceberg spec PR [apache/iceberg#15750], client PR [apache/iceberg#17337] |

---

> **Intent.** A proof of concept, not a merge candidate. Gravitino — a catalog
> with a real tag model (hierarchy, inheritance, assignment values) — is used to
> show that the proposed IRC `labels` field is a sufficient, useful projection
> surface for such a catalog. The **prototype is narrow** (producer read path);
> the **design is forward-looking** (§5 sketches the consumer and enforcement
> paths that the flat field enables but the POC does not build).

## 1. Background

Gravitino has two systems that don't currently meet:

- A **tag model**: named tags, associated to any object (catalog→schema→table→
  column), with assignment values (`pii = ssn`), value constraints, and
  **inheritance** already resolved server-side
  (`MetadataObjectTagOperations` walks `MetadataObjectUtil.getParentMetadataObjects`).
  Today tags are pure classification/discovery metadata — **not** used for
  access control, not synced to Ranger, not enforced anywhere.
- An **Iceberg REST catalog server**: external engines load Gravitino-managed
  tables over IRC, but the response carries none of that tag context.

Iceberg spec PR #15750 adds an optional `labels` field to `LoadTableResult` /
`LoadViewResult` for catalog-owned metadata enrichment. During review, a
maintainer cited Gravitino's tag model as *the* sophisticated example and asked
how a rich inheritance model reconciles with a flat read field. This POC answers
that concretely: **Gravitino resolves its own tag hierarchy and emits flat,
resolved values as `labels`.**

## 2. Goals

1. **Emit resolved tags as `labels`** on `loadTable` / `loadView`, matching the
   #15750 wire shape (`object-labels` + `fields`).
2. **Flatten inheritance server-side** (child-wins), so the consumer gets
   resolved values with no provenance on the wire.
3. **Carry assignment values** (`pii → ssn`, not `pii → ""`).
4. **Column labels keyed by `field-id`** (stable across rename), resolved from
   the loaded schema.
5. **Prove sufficiency**: Gravitino's full model projects into the flat shape
   with no loss that matters to a consuming engine.

## 3. Non-Goals

1. **Write path / consumer / enforcement** — out of the prototype (see §5 for the
   forward-looking design).
2. **Inheritance provenance, stable identity, rename reconciliation** — belong to
   the structured layer, not the flat read field.
3. **Non-Gravitino backends** — memory/JDBC/Hive passthrough have no Gravitino
   tags; they emit nothing.

## 4. Prototype design (producer read path)

### 4.1 Wire shape (matches #15750)

```json
"labels": {
  "object-labels": { "data_classification": "restricted" },
  "fields": [ { "field-id": 7, "labels": { "pii": "ssn" } } ]
}
```

### 4.2 Mapping

| Gravitino | IRC label |
|-----------|-----------|
| `Tag.name()` (direct or inherited) on table/view | key in `object-labels` |
| `Tag.assignment().values()` | value (joined if multiple) |
| tag on a column object | entry in `fields[]` |
| column name → `TableMetadata.schema().findField(name).fieldId()` | `field-id` |
| child key == ancestor key | child wins |

### 4.3 Seam & flow

```
IcebergTableOperations.loadTable  →  LoadTableResponse (+ TableMetadata → schema)
   │  metalake = IcebergRESTServerContext.metalakeName(); catalog = context.catalogName()
   ▼
TagLabelResolver           [NEW]  resolve direct+inherited tags, values, column→field-id
   │  via GravitinoEnv.getInstance().tagDispatcher() + getParentMetadataObjects
   ▼
attach "labels" to the response  →  HTTP 200 + ETag (ETag unchanged; labels are ephemeral)
```

Gated by config `gravitino.iceberg-rest.labels.enabled` (default `false`).

### 4.4 Dependency note

The `labels` field is not in released iceberg-core 1.11.0 (Gravitino's pin), so
the prototype **self-serializes** `labels` into the response JSON — it runs today
with no external dependency. When #15750 + #17337 land in a released iceberg,
swap the self-serializer for `LoadTableResponse.builder().withLabels(...)`; the
resolver logic is unchanged. (For an early demo against the genuine upstream
types, `publishToMavenLocal` from the PR stack and bump the `iceberg` version —
`mavenLocal()` is already on the build path.)

## 5. Forward-looking design (beyond the POC)

The flat read field is the substrate; richer stories build on it. None are in the
prototype, but the design should not preclude them.

### 5.1 Consumer path

Gravitino can also *consume* labels: as an IRC client (`catalog-backend=rest`),
it reads a foreign catalog's `labels` and re-materializes them as its own tags
(`createTag` if absent → `associateTagValuesForMetadataObject`).

> **Prototyped.** `LabelTagImporter` implements the core translate — governed
> match / create-ungoverned / fail-open on a disallowed value / idempotent —
> with column labels mapped back by `field-id`, and the foreign source recorded
> as a tag property (POC stand-in for the association-level column below).
> `CatalogLabelSyncService` is the idempotent per-catalog trigger body;
> `ForeignLabelSource` / `RestForeignLabelSource` read a foreign IRC catalog and
> extract `labels` via `IcebergLabels.fromJson` (bypassing the high-level client,
> which drops the field). Together this is the technical counter-evidence that a
> rich tag model *consumes* the flat field. What remains is wiring the trigger
> to an operator surface and to the federation config:

- A **maintenance command** (on-demand + cron), since Gravitino has no federation
  refresh loop. Idempotent: load → read labels → diff vs. last import → apply.
- A **`tag_source` marker** on the association (`tag_relation_meta`) to record
  origin and scope diff-removal to foreign tags only (so native tags are never
  deleted). Feasible, bounded schema change.
- A label is not an entity: consuming is always a **translate** (flat KV → local
  tag entity), lossy and **by name/value** — there is no stable id to reconcile
  on.

### 5.2 Enforcement & the rename boundary

Everything binds by **name**: `label key → Gravitino tag → Ranger tag → Ranger
policy`. Gravitino→Ranger is RBAC-only today; tag-based enforcement would need
Gravitino to feed a Ranger tag service, with the engine's Ranger plugin
enforcing.

The consequence: an upstream **rename** (`pii → pii_category`) has no stable id
on the flat wire, so Gravitino makes a new tag and drops the old one → any Ranger
policy bound to `pii` silently stops firing (**fail-open**). This is why the flat
field's contract is "the key *is* the identity," and why rename-safe enforcement
needs a **stable id from the structured layer** — from which a canonical Ranger
name is derived so renames can't break policy bindings. Ranger makes concrete
*why* identity belongs in the structured layer, not the read path.

## 6. Task breakdown

### POC (this doc)
- [ ] `TagLabelResolver` — identity bridge, direct+inherited resolution, values, table `object-labels`.
- [ ] Config `gravitino.iceberg-rest.labels.enabled` (default false).
- [ ] Self-serialize `labels` into the load response; wire into `IcebergTableOperations.loadTable`.
- [ ] Column labels via `field-id`; wire into `IcebergViewOperations.loadView`.
- [ ] Unit tests (inheritance flatten, value join, column→field-id, disabled/empty → omitted) + one IT.

### Consumer (this doc)
- [x] Consumer importer core (`LabelTagImporter`): governed/ungoverned, fail-open, field-id mapping, source marker + tests.
- [x] Trigger core: `CatalogLabelSyncService` (idempotent per-catalog sync), `ForeignLabelSource` + `RestForeignLabelSource` (raw IRC read + `IcebergLabels.fromJson`) + tests.
- [ ] Wire the trigger to a REST op / CLI / scheduler and to Gravitino's federated-catalog connection config (currently takes an explicit base URI).
- [ ] Association-level `tag_source` column on `tag_relation_meta`, so diff-removal is scoped to foreign tags only.

### Forward-looking (separate)
- [ ] Ranger tag-source integration (tag-based policy), keyed on stable identity from the structured layer.

[apache/iceberg#15750]: https://github.com/apache/iceberg/pull/15750
[apache/iceberg#17337]: https://github.com/apache/iceberg/pull/17337
