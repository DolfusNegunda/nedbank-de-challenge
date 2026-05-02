# Architecture Decision Record: Stage 3 Streaming Extension

**File:** `adr/stage3_adr.md`  
**Author:** Dolfus Negunda  
**Date:** 2026-05-02  
**Status:** Final

---

## Context

Stage 3 introduced a new requirement alongside the existing Stage 2 batch pipeline: process a directory of pre-staged micro-batch JSONL files from `/data/stream/` and maintain two new stream-facing Gold tables, `current_balances` and `recent_transactions`, with a five-minute SLA between source event time and output update time.

My Stage 2 architecture already separated heavy transaction processing from Spark by pushing the large Silver transactions transformation into DuckDB. That proved useful for Stage 3 because the stream files are also transaction-shaped data, only much smaller. Rather than introduce a new framework or a true continuous streaming engine, I extended the same architectural idea: Spark remains responsible for orchestration and Delta publication, while DuckDB handles row-wise transformation and compact state updates efficiently under the container’s tight resource constraints.

---

## Decision 1: How did the existing Stage 1 / Stage 2 architecture facilitate or hinder the streaming extension?

The most helpful design choice from the earlier stages was separating the pipeline into clear phases and making `run_all.py` the orchestrator rather than embedding all logic inside a single monolithic transformation script. By Stage 2, the pipeline already had an explicit multi-phase shape: Spark for Bronze, DuckDB for the heavy transaction transformation, and Spark again for Delta registration and Gold provisioning. That structure made it relatively straightforward to add a fourth phase for Stage 3 stream processing without disturbing the Stage 2 batch path.

The decision to move large transaction processing out of Spark in Stage 2 helped significantly. For Stage 3, I reused the same principle: use DuckDB to process stream files and maintain compact state tables, then use Spark only to publish those results as Delta. This kept the implementation consistent and avoided introducing a second large processing framework or a more complex true streaming engine inside a highly constrained container.

What hindered Stage 3 was that my Stage 2 design was still fundamentally batch-oriented. The original `run_all.py` assumed a finite sequence of transformations that terminate once Gold is written. Stage 3 added stateful incremental processing, which is conceptually different. I also did not centralize table schema definitions in a reusable module, so adding `current_balances` and `recent_transactions` required defining new output structures inside `stream_ingest.py` itself rather than referencing a shared schema registry.

Approximately 80–85% of the Stage 2 code survived unchanged into Stage 3. The core batch pipeline (`ingest.py`, `transform.py`, `provision.py`, `spark_session.py`) remained intact. The largest changes were a new implementation of `stream_ingest.py`, enabling the `streaming` block in configuration, and extending `run_all.py` to invoke the new phase.

---

## Decision 2: What design decisions in Stage 1 would I change in hindsight?

In hindsight, I would have introduced a more explicit boundary between **batch outputs** and **stateful outputs** earlier in the design. In Stage 1, the medallion model naturally drove me toward thinking in terms of Bronze, Silver, and Gold tables that are rewritten per run. That was appropriate for the batch problem, but it meant I did not initially model the idea of “persistent state updated incrementally,” which is the core of `current_balances` and `recent_transactions`. If I had anticipated Stage 3, I would have created a dedicated abstraction for state tables rather than treating all outputs as ordinary end-of-run tables.

I would also have centralized output schemas and table contracts in a single module. In my current design, schema intent is expressed across `provision.py`, `stream_ingest.py`, and the docs. That works, but it makes extension riskier. A shared schema registry would have made Stage 3 implementation faster and reduced the chance of drift between the intended contract and the actual output.

Finally, I would have introduced explicit pipeline mode handling earlier. Stage 2 still assumes the main entry point is a single linear pipeline. By Stage 3, that became “batch plus stream.” A cleaner design from the beginning would have been to make `run_all.py` mode-aware, with explicit handling for `batch`, `stream`, or `all`, instead of evolving the entry point incrementally.

---

## Decision 3: How would I approach this differently if I had known Stage 3 was coming from the start?

If I had known Stage 3 was part of the challenge from day one, I would have designed the pipeline around a **shared transaction processing core** that could serve both batch and stream inputs. Specifically, I would have built an abstraction where a transaction source — whether the full batch `transactions.jsonl` or a directory of micro-batch stream files — is normalized through one common transformation pathway, with the downstream target deciding whether the result becomes a Silver batch table, a Gold fact table, or an incremental state update.

I would also have designed state management explicitly from the beginning. `current_balances` and `recent_transactions` are not just more Gold outputs; they are rolling state representations. With that visibility, I would have made a deliberate distinction between **rebuildable analytical tables** and **incrementally maintained serving tables**. That likely would have resulted in a dedicated `state/` module or state-writer abstraction, rather than implementing that logic directly in `stream_ingest.py`.

From an orchestration perspective, I would have made `run_all.py` configuration-driven from day one, with an explicit concept of enabled phases. That would have avoided hard-wiring the assumption that the pipeline always ends after Gold provisioning. I still would likely have used DuckDB for transaction-heavy transformations because it proved efficient within the resource constraints, but I would have formalized that decision much earlier and structured the code to reflect it more cleanly.

---

## Appendix

### Module dependency model

```text
run_all.py
  ├── ingest.py                # Bronze
  ├── transform.py             # Silver batch
  ├── provision.py             # Gold batch + dq_report.json
  └── stream_ingest.py         # Stage 3 stream state + stream_gold outputs