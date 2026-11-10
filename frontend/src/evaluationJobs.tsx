import { useCallback, useEffect, useRef, useState } from "react";
import { ArrowRight, CircleAlert, LoaderCircle, RefreshCw } from "lucide-react";

type JobResult = { scenario: string; status: string; reason?: string };
type Job = { id: string; candidate: string; state: string; created_at: string; completed: number; total: number; current_scenario: string | null; current_fixture: {schema: string; scope: string} | null; results: JobResult[]; error: string | null; result_id: string | null };
type ApiRequest = <T>(token: string, path: string, init?: RequestInit) => Promise<T>;
type RequestError = Error & { status?: number };
type Pending = { key: string; candidate: string };
const PENDING_KEY = "ageis.pending-evaluation-job.v1";
const active = (job: Job) => job.state === "QUEUED" || job.state === "RUNNING";
const display = (value: string) => value.replaceAll("_", " ").replaceAll("::", " / ").toLowerCase();
function pendingFromStorage(): Pending | null {
  try {
    const raw = sessionStorage.getItem(PENDING_KEY);
    if (!raw) return null;
    const value = JSON.parse(raw);
    return typeof value.key === "string" && ["safe-v1", "unsafe-retry-v1"].includes(value.candidate) ? value : null;
  } catch { return null; }
}

export function EvaluationJobs({ token, request, onSessionExpired, onCompleted }: { token: string; request: ApiRequest; onSessionExpired: () => void; onCompleted: (resultId?: string) => void }) {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [candidate, setCandidate] = useState("safe-v1");
  const [pending, setPending] = useState<Pending | null>(pendingFromStorage);
  const [submitting, setSubmitting] = useState(false);
  const [actionBusy, setActionBusy] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const seenCompleted = useRef(new Set<string>());
  const fail = useCallback((err: unknown, fallback: string) => {
    if ((err as RequestError)?.status === 401) { onSessionExpired(); return; }
    setError(err instanceof Error ? err.message : fallback);
  }, [onSessionExpired]);
  const refresh = useCallback(async () => {
    try {
      const next = await request<Job[]>(token, "/evaluation-jobs?limit=50");
      setJobs(next);
      setSelected(old => old && next.some(job => job.id === old) ? old : next[0]?.id ?? null);
      let completedId: string | undefined;
      for (const job of next) if (job.state === "COMPLETED" && !seenCompleted.current.has(job.id)) { seenCompleted.current.add(job.id); completedId ??= job.result_id ?? undefined; }
      if (completedId) onCompleted(completedId);
      return next;
    } catch (err) { fail(err, "Could not load evaluation jobs."); return null; }
    finally { setLoading(false); }
  }, [token, request, fail, onCompleted]);
  useEffect(() => { void refresh(); }, [refresh]);
  useEffect(() => {
    if (!jobs.some(active)) return;
    const id = window.setInterval(() => { void refresh(); }, 2500);
    return () => window.clearInterval(id);
  }, [jobs, refresh]);
  const submit = async (event: React.FormEvent) => {
    event.preventDefault(); setError(""); setSubmitting(true);
    let item = pending;
    try {
      if (!item) {
        try {
          item = {key: crypto.randomUUID(), candidate};
          sessionStorage.setItem(PENDING_KEY, JSON.stringify(item));
        } catch {
          setError("Browser session storage is unavailable. A durable job cannot be started safely in this session.");
          return;
        }
        setPending(item);
      }
      const job = await request<Job>(token, "/evaluation-jobs", {method:"POST", headers:{"Idempotency-Key":item.key}, body:JSON.stringify({suite:"cross-v1",candidate:item.candidate})});
      sessionStorage.removeItem(PENDING_KEY); setPending(null);
      setSelected(job.id);
      setJobs(previous => [job, ...previous.filter(existing => existing.id !== job.id)]);
      await refresh();
    } catch (err) {
      if ((err as RequestError)?.status === 401) onSessionExpired();
      else setError((err as RequestError)?.status ? (err as Error).message : "Request outcome is unknown. Retry the same request; its saved key prevents a duplicate.");
    } finally { setSubmitting(false); }
  };
  const selectedJob = jobs.find(job => job.id === selected);
  const action = async (job: Job, kind: "cancel" | "continue-inconclusive") => {
    const message = kind === "cancel" ? "Cancel this job? An in-flight synthetic scenario may still send or finish a request. Its outcome will not be recorded as a success." : "Continue without replaying the interrupted scenario? It will be recorded as inconclusive and the remaining scenarios will run.";
    if (!window.confirm(message)) return;
    setActionBusy(true); setError("");
    try { await request<Job>(token, `/evaluation-jobs/${encodeURIComponent(job.id)}/${kind}`, {method:"POST"}); await refresh(); }
    catch (err) { fail(err, "Could not update job."); await refresh(); }
    finally { setActionBusy(false); }
  };
  return <section className="durable-jobs" aria-label="Durable evaluation jobs">
    <div className="eval-start"><div><div className="eyebrow">DURABLE SYNTHETIC EVALUATION</div><h2>Run a suite</h2><p>Jobs continue when you leave this page. Checkpoints and interrupted outcomes are saved.</p></div><form onSubmit={submit} className="durable-start-form"><label>Suite<input value="Access + notification" disabled/></label><label>Candidate<select value={pending?.candidate ?? candidate} onChange={e => setCandidate(e.target.value)} disabled={submitting || !!pending}><option value="safe-v1">Safe candidate</option><option value="unsafe-retry-v1">Unsafe retry · test regression</option></select></label><button className="primary" type="submit" disabled={submitting}>{submitting ? <><LoaderCircle className="spin" size={15}/> Sending…</> : <>{pending ? "Retry request" : "Start evaluation"}<ArrowRight size={15}/></>}</button></form>{pending && <p className="eval-progress" role="status">The last request has not been confirmed. Retrying uses the same idempotency key.</p>}</div>
    {error && <div className="notice error-notice durable-error" role="alert"><CircleAlert size={16}/><span>{error}</span><button type="button" aria-label="Dismiss job error" onClick={() => setError("")}>×</button></div>}
    <div className="durable-heading"><div><div className="eyebrow">DURABLE EXECUTION</div><h2>Evaluation jobs <span className="inline-count">{jobs.length}</span></h2><p>Latest 50 jobs. Completion means execution finished; check the evaluation for the results.</p></div><button className="secondary" type="button" onClick={() => {setError(""); void refresh();}} disabled={loading}><RefreshCw size={14} className={loading ? "spin" : ""}/> Refresh</button></div>
    <div className="durable-grid"><div className="durable-list" aria-label="Evaluation jobs">{loading && !jobs.length ? <p className="durable-empty">Loading jobs…</p> : !jobs.length ? <p className="durable-empty">No durable jobs yet.</p> : jobs.map(job => <button key={job.id} className={`durable-row ${selected === job.id ? "selected" : ""}`} onClick={() => setSelected(job.id)}><span><strong>{job.candidate === "safe-v1" ? "Safe candidate" : "Unsafe retry"}</strong><small>{job.id.slice(0,8).toUpperCase()} · {job.completed}/{job.total} scenarios</small></span><span className={`durable-state ${job.state.toLowerCase()}`}>{display(job.state)}</span></button>)}</div><div className="durable-detail">{selectedJob ? <><div className="durable-detail-head"><div><div className="eyebrow">JOB / {selectedJob.id.slice(0,8).toUpperCase()}</div><h3>{display(selectedJob.state)}</h3></div><span>{selectedJob.completed} / {selectedJob.total} recorded</span></div><progress max={selectedJob.total || 1} value={selectedJob.completed} aria-label="Recorded scenarios"/><p className="durable-description">{selectedJob.current_scenario ? `Current scenario: ${display(selectedJob.current_scenario)}` : selectedJob.state === "COMPLETED" ? "Execution finished. Open the evaluation below to inspect outcomes." : selectedJob.state === "QUEUED" ? "Waiting for an available worker." : "No scenario is currently in flight."}</p>{selectedJob.error && <p className="durable-error-detail"><CircleAlert size={15}/> {display(selectedJob.error)}</p>}{selectedJob.current_fixture && <p className="durable-fixture">Fixture retained for investigation: <code>{selectedJob.current_fixture.schema}</code> · <code>{selectedJob.current_fixture.scope}</code></p>}{selectedJob.results.length > 0 && <details className="durable-results"><summary>Recorded scenario checkpoints ({selectedJob.results.length})</summary>{selectedJob.results.map(item => <p key={item.scenario}><span>{display(item.scenario)}</span><strong>{display(item.status)}{item.reason ? ` · ${display(item.reason)}` : ""}</strong></p>)}</details>}<div className="durable-actions">{["QUEUED","RUNNING","INTERRUPTED"].includes(selectedJob.state) && <button type="button" className="secondary" disabled={actionBusy} onClick={() => void action(selectedJob, "cancel")}>Cancel job</button>}{selectedJob.state === "INTERRUPTED" && <button type="button" className="primary" disabled={actionBusy} onClick={() => void action(selectedJob, "continue-inconclusive")}>Continue as inconclusive</button>}{selectedJob.result_id && <button type="button" className="secondary" onClick={() => onCompleted(selectedJob.result_id ?? undefined)}>Open evaluation</button>}</div>{selectedJob.state === "INTERRUPTED" && <p className="durable-caution">The previous action may have completed. Continuing records it as inconclusive and does not replay it.</p>}{selectedJob.state === "CANCELLED" && selectedJob.current_fixture && <p className="durable-caution">An in-flight scenario may still send or finish a request. The fixture remains available for investigation.</p>}</> : <p className="durable-empty">Select a job to inspect its progress.</p>}</div></div>
  </section>;
}
