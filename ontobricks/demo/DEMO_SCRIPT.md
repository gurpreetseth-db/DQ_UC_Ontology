# Demo Script — "Meaning for humans *and* machines"

A ~10-minute Tier C walkthrough that lands OntoBricks as the natural next rung
above the Genie Ontology you already demo. Three acts.

---

## Act 0 — The bridge (30 sec)

> "You've seen NexusRetail's governed **Genie Ontology** — Discover domains,
> concept Pages, metric views — and Genie answering *'why is the return rate
> elevated?'* in natural language. That's meaning **for humans**. Now the same
> governed data, as a knowledge graph you can traverse and hand to an AI agent —
> meaning **for machines**."

Show the OntoBricks OntoViz canvas with the NexusRetail ontology (the classes map
1:1 to your concept Pages: Customer, Order, Product, Invoice, Return, Region).

---

## Act 1 — The graph is the story (3 min, in OntoBricks UI)

1. **Entity types** — show `list_entity_types`: total triples, Customer / Order /
   Product / Return counts. "This is your silver star schema, now as a graph."
2. **Find the batch** — search the `FAULT-PHON-2025Q3` **FaultyBatch** node.
3. **N-hop expansion** — expand neighbours: `FaultyBatch → Product (8 SKUs) →
   Order → Customer → Region`. The blast radius appears in one view. "No SQL, no
   joins remembered by hand — the relationships *are* the model."
4. **Cohort discovery** — run explainable cohort discovery from the batch node.
   It returns the impacted customers **with the path that explains why**.
5. **SHACL** — show the validation run is clean: "the graph itself proves the
   faulty-batch story is internally consistent" (then optionally break one
   mapping to show a violation).

---

## Act 2 — The agent reasons over it (5 min, via MCP)

Connect Claude (Code/Desktop) or the Databricks Playground to the published
`nexusretail` domain (see [`../mcp/README.md`](../mcp/README.md)). Run these
prompts verbatim — each maps to specific MCP tools:

1. **Orient** —
   > "List the available OntoBricks domains and select the NexusRetail one, then
   > describe its ontology."
   *(→ `list_domains` → `select_domain` → `describe_ontology`)*

2. **Traverse** —
   > "Starting from the faulty batch FAULT-PHON-2025Q3, which products belong to
   > it, and which customers ordered those products? Group the customers by
   > region."
   *(→ `describe_entity` / `query_graphql` — the graph traversal)*

3. **Compute live (virtual attribute)** —
   > "For customer <id> from that cohort, what is their return-risk score and how
   > many orders exposed them to the faulty batch?"
   *(→ `compute_virtual_attributes` → `customer_return_risk_score`,
   `customer_faulty_batch_exposure`)*

4. **Act (entity action)** —
   > "Run the affected-customer cohort action for product FAULT-PHON-4 and tell me
   > which of them have not yet returned it."
   *(→ `invoke_entity_action` → `product_affected_cohort`)*

5. **Rollup** —
   > "Which region had the largest faulty-batch refund exposure?"
   *(→ `invoke_entity_action` → `region_faulty_batch_impact`, expect APAC-East)*

**The payoff line:** "The agent didn't guess — it reasoned over a *governed*
graph, called *governed* Unity Catalog functions, and every answer is explainable
back to a triple and a table."

---

## Act 3 — Why this matters (1 min)

| Genie Ontology (you already have) | OntoBricks graph (this) |
|---|---|
| Human-modelled: domains, Pages, metrics | Machine-reasonable: triples, OWL, SHACL |
| Genie cites concepts as context | Agents traverse relationships + call functions |
| Answers "what/how much" over metrics | Answers "how are these connected / who is impacted" |
| Governed tags + comments | Governed DRAFT→PUBLISHED domain, MCP policy |

> "Same governed data. Two layers of meaning. Genie for the analyst,
> the knowledge graph + MCP for the AI agent."

---

## Fallback (if a live deploy isn't ready)

Demo Acts 0 and 3 as slides, and screen-record Acts 1–2 ahead of time — the
graph build (Auto-Map + Synchronize) runs for minutes and is billable, so
pre-building the `nexusretail` domain before the session is recommended
regardless.
