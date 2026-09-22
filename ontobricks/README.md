# OntoBricks Companion Module — NexusRetail Knowledge Graph

This module extends the NexusRetail demo with a **knowledge graph** layer built
on [**OntoBricks**](https://github.com/databrickslabs/ontobricks) (Databricks
Labs). It is the machine-reasonable counterpart to the **Genie Ontology** the
rest of this repo already showcases.

```
 gold → metrics  ──►  Genie Ontology        meaning for HUMANS
 (this repo)          (domains, Pages,       — Genie answers "what / how much"
                       metric views)

 silver star     ──►  OntoBricks graph       meaning for MACHINES
 schema               (triples, OWL, SHACL,  — agents traverse "how connected /
 (this repo)           GraphQL, MCP)           who's impacted" + call UC functions
```

> **OntoBricks is a separate Databricks Labs app** (clone + `make deploy`), not
> part of this bundle. This module holds the *curated artefacts* and the
> *runbook* that make OntoBricks tell the NexusRetail story consistently with the
> Genie concept Pages — nothing here changes the existing bundle or pipeline.

## What's here

| Path | What it is |
|---|---|
| [`RUNBOOK.md`](./RUNBOOK.md) | **Start here.** End-to-end deploy + build + publish + demo, incl. Lakebase Autoscaling provisioning. |
| [`ontology/nexusretail.ttl`](./ontology/nexusretail.ttl) | Hand-authored OWL/Turtle ontology matching the Genie concept Pages. Import into OntoViz in one click. |
| [`ontology/MAPPINGS.md`](./ontology/MAPPINGS.md) | Every class/property → silver table + column, to guide Auto-Map or author R2RML. |
| [`shacl/faulty_batch_shapes.ttl`](./shacl/faulty_batch_shapes.ttl) | SHACL shapes validating the faulty-batch narrative — the graph-native complement to DQX. |
| [`uc_functions/virtual_attributes.sql`](./uc_functions/virtual_attributes.sql) | Read-only UC functions exposed to agents as MCP virtual attributes + actions (return risk, cohort, region impact). |
| [`mcp/`](./mcp/) | MCP client config template + how to connect Claude Code / Desktop / Playground to the published graph. |
| [`demo/DEMO_SCRIPT.md`](./demo/DEMO_SCRIPT.md) | ~10-min three-act talk track (graph → agent → why it matters). |

## The one demo idea

The Q4-2025 **FAULT-PHON-*** return spike — already the narrative spine of this
demo — becomes a **graph traversal** and an **agentic reasoning loop**:

```
FaultyBatch(FAULT-PHON-2025Q3) ◄─ Product ◄─ Order ─► Customer ─► Region(APAC-East)
```

An AI agent, over MCP, walks that path, computes each customer's live return-risk
score with a governed UC function, and rolls up refund exposure by region — every
answer explainable back to a triple and a table.

## Prerequisites at a glance

Databricks Apps enabled · a serverless SQL Warehouse · Model Serving · a
**Lakebase Autoscaling** database (not present in this workspace yet — see the
runbook) · a UC Volume. Full checklist and commands in [`RUNBOOK.md`](./RUNBOOK.md).
