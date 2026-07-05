# 0001 — Query endpoint trusts `namespace_id` from the request body (IDOR / cross-tenant access)

- **Severity:** Critical (before any non-trusted exposure)
- **Status:** Open — known v1 stub
- **Component:** `crates/ukiel-query/src/server.rs`
- **Found by:** automated security review, 2026-07-05

## Summary

`POST /api/query` reads `namespace_id` directly from the client-supplied JSON
body and builds the query session for that namespace. There is no
authentication or authorization: any caller can set `namespace_id` to any value
and read that tenant's data. This is a classic IDOR / cross-tenant access
vulnerability if the endpoint is exposed to untrusted callers.

```rust
#[derive(serde::Deserialize)]
pub struct QueryRequest {
    pub namespace_id: i64, // caller-controlled — trust boundary violation
    pub sql: String,
}

async fn run_query(State(state): State<AppState>, Json(req): Json<QueryRequest>) -> ... {
    let ctx = session_for_namespace(&state.catalog, NamespaceId(req.namespace_id), ...).await?;
    // ...
}
```

## Why it exists (v1 context)

This is an **intentional v1 stub**, not an accidental bug:

- Plan 3 (`docs/superpowers/plans/2026-07-05-ukiel-v1-query.md`, Task 5) defines
  the endpoint contract as `{"namespace_id": 1, "sql": "..."}` verbatim.
- The design spec (`docs/superpowers/specs/2026-07-05-ukiel-design.md`) scopes v1
  to "prove the core: write → catalog → query," with auth/quotas in the
  "designed-for, stubbed" bucket.

The **isolation mechanism** is implemented and tested: a session built for
namespace N can only ever see N's rows (the packing-key filter is injected into
the physical plan upstream of any projection — see
`namespace_sees_only_its_rows_even_in_packed_files` and
`projection_excluding_packing_key_still_isolates` in
`crates/ukiel-query/tests/query_test.rs`). What is missing is
**authenticating which namespace the caller is**, i.e. establishing the trusted
`NamespaceId` instead of taking it from the request body.

## Impact

Until fixed, the endpoint must only be reachable by fully trusted callers
(e.g. a trusted gateway that has already authenticated the tenant). Any direct
exposure allows one tenant to read every other tenant's data by changing one
integer.

## Fix

Derive `NamespaceId` from an **authenticated principal**, never from the body:

1. Add an axum extractor / middleware that authenticates the caller and yields
   the authorized `NamespaceId`. Mechanism is a deployment decision — one of:
   JWT claim, session token, mTLS client identity, or a trusted-proxy header
   verified against a shared secret.
2. Remove `namespace_id` from `QueryRequest`; source it from the extractor.
3. If a caller may be authorized for more than one namespace, validate the
   requested namespace against the principal's grants before calling
   `session_for_namespace`.

`session_for_namespace()` is already the correct seam — it is the single point
where the namespace becomes the isolation scope, so the auth layer slots in
cleanly in front of it with no changes to the catalog/provider isolation logic.

## Notes

- Do not treat "internal-only service" as mitigation: internal services are
  common IDOR/SSRF targets.
- Tracking as a dedicated post-v1 plan item (real authn/authz), since the auth
  mechanism choice is an owner decision that changes the endpoint's contract.
