import { useCallback, useEffect, useState } from "react";
import { ArrowRight, CircleAlert, GitCompareArrows, LoaderCircle, RefreshCw, ShieldCheck } from "lucide-react";

type Counts = { passed: number; failed: number; error: number; inconclusive: number };
type Summary = { id: string; suite_id: string; candidate: string; status: string; created_at: string; counts: Counts };
type Scenario = { scenario: string; status: string; failed_assertions: string[]; reason?: string; error_category?: string; duration_seconds?: number; evidence?: { fault_injections?: string[]; mock_evidence?: Record<string, unknown>; events?: unknown[]; run_id?: string } };
type Evaluation = { id: string; created_at: string; manifest: { suite_id: string; candidate: string; suite_hash?: string }; counts: Counts; results: Scenario[]; evidence_hash: string };
type Comparison = { id: string; evidence_hash: string; baseline: { id: string; manifest: { candidate: string } }; candidate: { id: string; manifest: { candidate: string } }; changes: { scenario: string; baseline: string; candidate: string; baseline_failed_assertions: string[]; candidate_failed_assertions: string[]; baseline_reason?: string | null; candidate_reason?: string | null }[]; warnings: string[]; deployment_performed: boolean };
type Decision = { id: string; decision: string; actor: string; created_at: string; deployment_performed: boolean };
type ApiRequest = <T>(token: string, path: string, init?: RequestInit) => Promise<T>;
type RequestError = Error & { status?: number };
const statusNames: Record<string, string> = { passed: "Passed", failed: "Failed", error: "Error", inconclusive: "Inconclusive" };
const label = (value: string) => value.replaceAll("_", " ").replaceAll("::", " / ");
const short = (id: string) => id.slice(0, 8).toUpperCase();
const formatTime = (value: string) => { const date = new Date(value); return Number.isNaN(date.getTime()) ? "—" : new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", hour12: false }).format(date); };
function State({ value }: { value: string }) { return <span className={`eval-state ${value}`}><span />{statusNames[value] ?? label(value)}</span>; }
function Totals({ counts }: { counts: Counts }) { return <div className="eval-totals" aria-label="Scenario counts">{(["passed", "failed", "error", "inconclusive"] as const).map(status => <div key={status}><span>{statusNames[status]}</span><strong className={status}>{counts[status]}</strong></div>)}</div>; }

export function Evaluations({ token, request, onSessionExpired }: { token: string; request: ApiRequest; onSessionExpired: () => void }) {
  const [runs, setRuns] = useState<Summary[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [detail, setDetail] = useState<Evaluation | null>(null);
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [error, setError] = useState("");
  const [running, setRunning] = useState(false);
  const [suite, setSuite] = useState("cross-v1");
  const [candidate, setCandidate] = useState("safe-v1");
  const [baselineId, setBaselineId] = useState("");
  const [candidateId, setCandidateId] = useState("");
  const [comparison, setComparison] = useState<Comparison | null>(null);
  const [comparisonId, setComparisonId] = useState("");
  const [decision, setDecision] = useState<Decision | null>(null);
  const [decisionReady, setDecisionReady] = useState(false);
  const [comparisonBusy, setComparisonBusy] = useState(false);
  const [decisionBusy, setDecisionBusy] = useState(false);
  const [decisionChoice, setDecisionChoice] = useState("needs_review");

  const fail = useCallback((err: unknown, fallback: string) => {
    if ((err as RequestError)?.status === 401) { onSessionExpired(); return; }
    setError(err instanceof Error ? err.message : fallback);
  }, [onSessionExpired]);
  const refresh = useCallback(async (preferred?: string) => {
    setLoading(true);
    try {
      const next = await request<Summary[]>(token, "/evaluations?limit=50");
      setRuns(next);
      setSelected(old => preferred || (old && next.some(run => run.id === old) ? old : next[0]?.id ?? null));
    } catch (err) { fail(err, "Could not load evaluations."); }
    finally { setLoading(false); }
  }, [token, request, fail]);
  useEffect(() => { void refresh(); }, [refresh]);
  useEffect(() => {
    if (!selected) { setDetail(null); return; }
    let active = true; setDetail(null); setDetailLoading(true);
    request<Evaluation>(token, `/evaluations/${encodeURIComponent(selected)}`).then(value => { if (active) setDetail(value); })
      .catch(err => { if (active) fail(err, "Could not load evaluation."); })
      .finally(() => { if (active) setDetailLoading(false); });
    return () => { active = false; };
  }, [selected, token, request, fail]);

  const start = async (event: React.FormEvent) => {
    event.preventDefault(); setRunning(true); setError("");
    try {
      const created = await request<{ id: string }>(token, "/evaluations", { method: "POST", body: JSON.stringify({ suite, candidate }) });
      await refresh(created.id);
    } catch (err) {
      if (!(err as RequestError)?.status) setError("Connection interrupted. Refresh the list before retrying; the server may still be working.");
      else fail(err, "Evaluation request failed.");
    } finally { setRunning(false); }
  };
  const loadDecision = async (id: string) => {
    setDecision(null); setDecisionReady(false);
    try { setDecision(await request<Decision>(token, `/evaluation-comparisons/${encodeURIComponent(id)}/decision`)); setDecisionReady(true); }
    catch (err) { if ((err as RequestError)?.status === 404) setDecisionReady(true); else fail(err, "Could not load decision."); }
  };
  const compare = async (event: React.FormEvent) => {
    event.preventDefault(); setComparisonBusy(true); setError(""); setComparison(null); setDecision(null); setDecisionReady(false);
    try {
      const value = await request<Comparison>(token, "/evaluation-comparisons", { method: "POST", body: JSON.stringify({ baseline_id: baselineId, candidate_id: candidateId }) });
      setComparison(value); setComparisonId(value.id); await loadDecision(value.id);
    } catch (err) { fail(err, "Could not compare evaluations."); }
    finally { setComparisonBusy(false); }
  };
  const openComparison = async (event: React.FormEvent) => {
    event.preventDefault(); if (!comparisonId.trim()) return; setComparisonBusy(true); setError(""); setComparison(null); setDecision(null); setDecisionReady(false);
    try {
      const value = await request<Comparison>(token, `/evaluation-comparisons/${encodeURIComponent(comparisonId.trim())}`);
      setComparison(value); setComparisonId(value.id); await loadDecision(value.id);
    } catch (err) { fail(err, "Could not open comparison."); }
    finally { setComparisonBusy(false); }
  };
  const recordDecision = async (event: React.FormEvent) => {
    event.preventDefault(); if (!comparison || !decisionReady) return; setDecisionBusy(true); setError("");
    try { setDecision(await request<Decision>(token, `/evaluation-comparisons/${encodeURIComponent(comparison.id)}/decision`, { method: "POST", body: JSON.stringify({ decision: decisionChoice }) })); }
    catch (err) { fail(err, "Could not record decision."); await loadDecision(comparison.id); }
    finally { setDecisionBusy(false); }
  };
  return <div className="eval-page">
    {error && <div className="notice error-notice" role="alert"><CircleAlert size={16}/><span>{error}</span><button aria-label="Dismiss error" onClick={() => setError("")}>×</button></div>}
    <section className="eval-start"><div><div className="eyebrow">SYNTHETIC EVALUATION</div><h2>Run a suite</h2><p>Exercise trusted workflows against controlled faults. Results are saved when the suite finishes.</p></div><form onSubmit={start} className="eval-start-form"><label>Suite<select value={suite} onChange={e => setSuite(e.target.value)} disabled={running}><option value="cross-v1">Access + notification</option><option value="access-v1">Access only</option></select></label><label>Candidate<select value={candidate} onChange={e => setCandidate(e.target.value)} disabled={running}><option value="safe-v1">Safe candidate</option><option value="unsafe-retry-v1">Unsafe retry · test regression</option></select></label><button className="primary" type="submit" disabled={running}>{running ? <><LoaderCircle className="spin" size={15}/> Running…</> : <>Run evaluation <ArrowRight size={15}/></>}</button></form>{running && <p className="eval-progress" role="status">Running the suite. This can take tens of seconds; keep this page open. If the request is interrupted, refresh the list before retrying.</p>}</section>
    <div className="section-heading eval-heading"><div><div className="eyebrow">RECORDED EVIDENCE</div><h2>Evaluation history <span className="inline-count">{runs.length}</span></h2><p>Showing up to the latest 50 completed evaluations.</p></div><button className="secondary" onClick={() => { setError(""); void refresh(); }} disabled={loading}><RefreshCw size={14} className={loading ? "spin" : ""}/> Refresh</button></div>
    <div className="eval-grid"><section className="eval-list" aria-label="Evaluation history">{loading && !runs.length ? <div className="eval-empty"><LoaderCircle className="spin" size={18}/> Loading evaluations</div> : !runs.length ? <div className="eval-empty"><GitCompareArrows size={20}/><strong>No evaluations yet</strong><span>Run a suite to create the first record.</span></div> : runs.map(run => <button key={run.id} className={`eval-row ${selected === run.id ? "selected" : ""}`} onClick={() => setSelected(run.id)}><div><strong>{run.candidate === "safe-v1" ? "Safe candidate" : run.candidate === "unsafe-retry-v1" ? "Unsafe retry" : run.candidate}</strong><small>{run.suite_id} · {short(run.id)}</small></div><div className="eval-row-end"><State value={run.status}/><small>{formatTime(run.created_at)}</small></div></button>)}</section>
    <section className="eval-detail" aria-label="Evaluation details">{detailLoading ? <div className="eval-empty"><LoaderCircle className="spin" size={18}/> Loading evidence</div> : !detail ? <div className="eval-empty"><span>Select an evaluation to inspect its evidence.</span></div> : <><div className="eval-detail-head"><div><div className="eyebrow">EVALUATION / {short(detail.id)}</div><h3>{detail.manifest.candidate === "safe-v1" ? "Safe candidate" : detail.manifest.candidate === "unsafe-retry-v1" ? "Unsafe retry" : detail.manifest.candidate}</h3><p>{detail.manifest.suite_id} · {formatTime(detail.created_at)}</p></div><span className="eval-recorded">RECORDED</span></div><Totals counts={detail.counts}/><div className="eval-scenario-title"><div><div className="eyebrow">SCENARIO RESULTS</div><h4>Evidence <span>{detail.results.length}</span></h4></div><span>EXPAND TO INSPECT</span></div><div className="eval-scenarios">{detail.results.map(result => <details key={result.scenario} className="eval-scenario"><summary><span><strong>{label(result.scenario)}</strong><small>{result.duration_seconds !== undefined ? `${result.duration_seconds.toFixed(2)} s` : "Duration unavailable"}</small></span><State value={result.status}/></summary><div className="eval-evidence">{result.failed_assertions.length > 0 && <p><b>Failed assertions</b> {result.failed_assertions.map(label).join(" · ")}</p>}{result.reason && <p><b>Reason</b> {label(result.reason)}{result.error_category ? ` · ${label(result.error_category)}` : ""}</p>}{result.evidence ? <><p><b>Faults injected</b> {result.evidence.fault_injections === undefined ? "Not available for this suite" : result.evidence.fault_injections.length ? result.evidence.fault_injections.map(label).join(" · ") : "None"}</p>{result.evidence.run_id && <p><b>Run ID</b> <code>{result.evidence.run_id}</code></p>}<details className="eval-raw"><summary>Service evidence and events</summary><pre>{JSON.stringify({ service: result.evidence.mock_evidence ?? null, events: result.evidence.events ?? [] }, null, 2)}</pre></details></> : <p>No scenario evidence was captured.</p>}</div></details>)}</div><div className="eval-hash">EVIDENCE HASH <code>{detail.evidence_hash}</code></div></>}</section></div>
    <section className="eval-compare"><div className="eval-compare-head"><div><div className="eyebrow">RELEASE EVIDENCE</div><h2>Compare evaluations</h2><p>Compare compatible runs and record a human decision. No deployment is performed.</p></div><GitCompareArrows size={23}/></div><form className="eval-compare-form" onSubmit={compare}><label>Baseline<select value={baselineId} onChange={e => setBaselineId(e.target.value)} required><option value="">Select an evaluation</option>{runs.map(run => <option key={run.id} value={run.id}>{run.candidate} · {run.suite_id} · {short(run.id)}</option>)}</select></label><label>Candidate<select value={candidateId} onChange={e => setCandidateId(e.target.value)} required><option value="">Select an evaluation</option>{runs.map(run => <option key={run.id} value={run.id}>{run.candidate} · {run.suite_id} · {short(run.id)}</option>)}</select></label><button className="secondary" type="submit" disabled={comparisonBusy || !baselineId || !candidateId}>{comparisonBusy ? "Working…" : "Compare"}<ArrowRight size={14}/></button></form><form className="eval-open-form" onSubmit={openComparison}><label htmlFor="comparison-id">Open a saved comparison by ID</label><div><input id="comparison-id" value={comparisonId} onChange={e => setComparisonId(e.target.value)} placeholder="Comparison ID"/><button className="text-button" type="submit" disabled={comparisonBusy || !comparisonId.trim()}>Open <ArrowRight size={14}/></button></div></form>
      {comparison && <div className="eval-comparison-result"><div className="eval-comparison-heading"><div><div className="eyebrow">COMPARISON / {short(comparison.id)}</div><h3>{comparison.baseline.manifest.candidate} <span>→</span> {comparison.candidate.manifest.candidate}</h3><p>Comparison ID: <code>{comparison.id}</code></p></div><span className="eval-recorded">SAVED</span></div>{comparison.warnings.length > 0 && <div className="eval-warning"><CircleAlert size={15}/><span>{comparison.warnings.join(" · ")}</span></div>}<div className="eval-change-header"><span>SCENARIO</span><span>BASELINE</span><span>CANDIDATE</span></div>{comparison.changes.map(change => <div className="eval-change" key={change.scenario}><div><strong>{label(change.scenario)}</strong>{(change.baseline_failed_assertions.length > 0 || change.baseline_reason) && <small>Baseline: {change.baseline_failed_assertions.map(label).join(" · ")}{change.baseline_reason ? ` ${label(change.baseline_reason)}` : ""}</small>}{(change.candidate_failed_assertions.length > 0 || change.candidate_reason) && <small>Candidate: {change.candidate_failed_assertions.map(label).join(" · ")}{change.candidate_reason ? ` ${label(change.candidate_reason)}` : ""}</small>}</div><State value={change.baseline}/><State value={change.candidate}/></div>)}<div className="eval-decision"><div><ShieldCheck size={17}/><div><strong>Human decision</strong><p>One decision may be recorded. It does not deploy a candidate.</p></div></div>{decision ? <p className="eval-decision-record"><State value={decision.decision}/> Recorded by {decision.actor} · {formatTime(decision.created_at)}</p> : decisionReady ? <form onSubmit={recordDecision}><label className="sr-only" htmlFor="eval-decision">Decision</label><select id="eval-decision" value={decisionChoice} onChange={e => setDecisionChoice(e.target.value)}><option value="needs_review">Needs review</option><option value="approved">Approved</option><option value="rejected">Rejected</option></select><button className="primary" type="submit" disabled={decisionBusy}>{decisionBusy ? "Recording…" : "Record decision"}</button></form> : <p className="eval-decision-record">Decision status unavailable. Reopen the comparison to retry.</p>}</div></div>}
    </section>
  </div>;
}
