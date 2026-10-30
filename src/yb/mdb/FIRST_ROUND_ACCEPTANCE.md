# MDB first-round acceptance checkpoint

Date: 2026-10-30. Product source checkpoint: `370c311ade34177788e2274f39583788799580fd`.
This document closes the agreed bounded repair/acceptance round. It does not
declare every goal in the original thirteen-item roadmap complete.

## Accepted scope and existing evidence

| Scope | Evidence and boundary |
| --- | --- |
| Initial result-correctness repairs | First-batch report: 21 distinct GTests and 77 queries across three execution configurations, covering row/unbound results, VALUES, MINUS, property paths, ASK, CONSTRUCT and RDF term identity. Historical evidence; not rerun for this document. |
| Local scale and concurrent recovery | Five targeted Release tests at ancestor `8368b5fee60`; includes complete typed bags, two-client isolation and recovery. Bounded workloads, not production capacity. |
| Cross-host storage changes | Two storage hosts, three tablet servers, RF3 and one master. Active queries survived an actual leader transfer and tablet split. Fourteen successful complete-result observations plus one expected SSH-worker-channel failure with empty stdout and subsequent recovery. Source `8368b5fee60`. |
| Initialization optimization | Ten targeted Release regressions and 36 complete-result observations; committed source hashes matched checkpoint `370c311ade34`. Startup medians decreased 38-48% on the tested synthetic shapes; not a join-speed claim. |
| Unified runtime | CLI, worker and private dependencies matched checkpoint build hashes. Twelve representative queries and an alternating worker comparison produced 48 correct complete-result observations. Stable executable/library manifest and rollback runtime retained. |
| Reference comparison | 288 complete-result observations, including 240 measured samples, against QLever 0.6.0. Reference-only: MDB RF3 and single-node QLever have different guarantees and entry lifetimes. The comparison used the new CLI with the earlier worker; it is not an all-current-build performance ranking. |

Reports, raw responses, command records, input/oracle hashes and runtime manifests
are retained in the operator's project worklog and evidence archives. Relevant
archive groups are `first-batch-fix-20261027`, `stage13-local-20261029`,
`crosshost-acceptance-20261030`, `crosshost-nextstep-20261030`,
`formal-comparison-20261030` and `runtime-unification-20261030`.
Counts above describe separate experiments and must not be combined into a
single test-suite or same-version benchmark claim.

## Excluded experimental change

Worker-local DISTINCT prefiltering is preserved separately from this checkpoint.
Its six regressions and 49 complete-result observations passed. Fanout protocol
response bytes fell from 1,604,155 to 34,828, but measured end-to-end medians did
not improve. Worker/coordinator communication was local to one cloud host, not a
cross-host worker link. The experimental switch remains off by default and the
stable cloud runtime was not replaced. No performance benefit is claimed here.

## Known limits and next-round work

- Ordered windows without DISTINCT still receive all intermediate worker rows.
  Any candidate pushdown must use RDF value ordering, not persistent-ID order.
- Exchange consumers remain at the coordinator. General worker-to-worker
  exchange, broader multi-stage plans and unbounded external operations remain
  outside this acceptance round.
- Strategy advice and data locality are not a fully validated general optimizer.
  SSH loopback mappings do not validate real-address locality decisions.
- One master and a two-host 2+1 replica layout do not establish master HA or
  availability after either host fails. Long-duration stress, physical-node
  scaling, broader failure modes and production resource limits remain unverified.
- One later setup timed out during import before cloud leaders were pinned.
  Pinning leaders before import allowed the bounded validation to complete;
  arbitrary WAN-leader import stability was not fixed or accepted.
- Reference COUNT results had matching values but different exported RDF
  datatypes (`xsd:int` versus `xsd:integer`); COUNT was excluded from the reference
  performance set without coercion.

## Reproduction and release discipline

The closeout changes documentation only. Reuse the recorded validation rather
than rebuilding or rerunning tests just to publish this checkpoint. Runtime
identity is determined by file hashes and source provenance, not version text
alone. After switching from the experimental branch, an existing local build
directory may still contain experimental binaries; rebuild the intended targets
before using that directory for a new run or deployment.

No persistent-format migration, default-strategy change, new join algorithm or
additional cloud service is introduced by this closeout. Existing runtimes and
historical experiment artifacts are retained for rollback and reproduction.
