/**
 * Codeace single-page application.
 *
 * Top-level App component owns all auth and session state. Child page
 * components (AuthPage, UploadPage, DashboardPage) receive only the slice of
 * state they need via props — no context or global store is used.
 *
 * WebSocket connections are stored in wsRefs (a ref-of-map) so they survive
 * re-renders without triggering them. Each job gets one WS connection that is
 * closed on logout or token expiry.
 */
import React, { useEffect, useMemo, useRef, useState } from "react";
import Plotly from "plotly.js-dist-min";
import { api, wsUrl } from "./api";

const defaultFilters = {
  event_type: "",
  brand: "",
  category: "",
  min_price: "",
  max_price: "",
  start_time: "",
  end_time: "",
  page: 1,
  page_size: 10,
};

export default function App() {
  // ── Auth ──────────────────────────────────────────────────────────────────
  const [authState, setAuthState] = useState("checking"); // checking | unauthenticated | authenticated
  const [currentUser, setCurrentUser] = useState(null);

  // Validate stored token on mount
  useEffect(() => {
    const token = localStorage.getItem("codeace_token");
    if (!token) {
      setAuthState("unauthenticated");
      return;
    }
    api.get("/api/auth/me")
      .then(({ data }) => {
        setCurrentUser(data);
        setAuthState("authenticated");
      })
      .catch(() => {
        localStorage.removeItem("codeace_token");
        setAuthState("unauthenticated");
      });
  }, []);

  // Handle 401 responses emitted by the axios interceptor
  useEffect(() => {
    const handler = () => {
      setCurrentUser(null);
      setAuthState("unauthenticated");
      setSessions([]);
      setActiveJobId(null);
      setView("upload");
      Object.values(wsRefs.current).forEach((ws) => ws.close());
      wsRefs.current = {};
    };
    window.addEventListener("codeace:unauthorized", handler);
    return () => window.removeEventListener("codeace:unauthorized", handler);
  }, []);

  function handleAuth(user) {
    setCurrentUser(user);
    setAuthState("authenticated");
  }

  function handleLogout() {
    localStorage.removeItem("codeace_token");
    setCurrentUser(null);
    setAuthState("unauthenticated");
    setSessions([]);
    setActiveJobId(null);
    setView("upload");
    Object.values(wsRefs.current).forEach((ws) => ws.close());
    wsRefs.current = {};
  }

  // ── Sessions & dashboard state ────────────────────────────────────────────
  const [view, setView] = useState("upload");
  const [sessions, setSessions] = useState([]);
  const [activeJobId, setActiveJobId] = useState(null);
  const [metrics, setMetrics] = useState(null);
  const [events, setEvents] = useState(null);
  const [anomalies, setAnomalies] = useState(null);
  const [prompt, setPrompt] = useState("");
  const [answer, setAnswer] = useState(null);
  const [busyPrompt, setBusyPrompt] = useState(false);
  const [assistantError, setAssistantError] = useState("");
  const [uploadErrors, setUploadErrors] = useState([]);
  const [filters, setFilters] = useState(defaultFilters);
  const wsRefs = useRef({});
  const loadedJobRef = useRef(null);

  function openWebSocket(jobId) {
    if (wsRefs.current[jobId]) return;
    const ws = new WebSocket(wsUrl(jobId));
    ws.onmessage = (e) => {
      const data = JSON.parse(e.data);
      setSessions((prev) => prev.map((s) => (s.jobId === jobId ? { ...s, ...data } : s)));
    };
    ws.onclose = (e) => {
      if (e.code === 4001) {
        // Server rejected WS due to invalid/expired token
        localStorage.removeItem("codeace_token");
        window.dispatchEvent(new Event("codeace:unauthorized"));
      }
      delete wsRefs.current[jobId];
    };
    wsRefs.current[jobId] = ws;
  }

  useEffect(() => {
    if (!activeJobId) return;
    const session = sessions.find((s) => s.jobId === activeJobId);
    if (session?.status === "completed" && loadedJobRef.current !== activeJobId) {
      loadedJobRef.current = activeJobId;
      loadDashboard(activeJobId);
    }
  }, [activeJobId, sessions]);

  async function uploadFiles(fileList, inputEl) {
    const files = Array.from(fileList);
    if (!files.length) return;
    const newErrors = [];
    await Promise.allSettled(
      files.map(async (file) => {
        const form = new FormData();
        form.append("file", file);
        try {
          const { data } = await api.post("/api/uploads", form, {
            headers: { "Content-Type": "multipart/form-data" },
          });
          setSessions((prev) => [
            ...prev,
            { jobId: data.job_id, filename: file.name, status: "queued", progress: 0 },
          ]);
          openWebSocket(data.job_id);
        } catch (err) {
          const detail = err.response?.data?.detail;
          newErrors.push(`${file.name}: ${typeof detail === "string" ? detail : "Upload failed."}`);
        }
      })
    );
    if (inputEl) inputEl.value = "";
    setUploadErrors(newErrors);
  }

  async function loadDashboard(jobId) {
    const [mr, er, ar] = await Promise.all([
      api.get(`/api/jobs/${jobId}/metrics`),
      api.get(`/api/jobs/${jobId}/events`, { params: { page: 1, page_size: defaultFilters.page_size } }),
      api.get(`/api/jobs/${jobId}/anomalies`),
    ]);
    setMetrics(mr.data);
    setEvents(er.data);
    setAnomalies(ar.data);
  }

  async function loadEvents(nextFilters = filters) {
    if (!activeJobId) return;
    const params = Object.fromEntries(
      Object.entries(nextFilters).filter(([, v]) => v !== "" && v !== null && v !== undefined)
    );
    const response = await api.get(`/api/jobs/${activeJobId}/events`, { params });
    setEvents(response.data);
  }

  function updateFilter(name, value) {
    const next = { ...filters, [name]: value, page: 1 };
    setFilters(next);
    loadEvents(next);
  }

  function changePage(nextPage) {
    const totalPages = events?.total_pages || 1;
    const safe = Math.min(Math.max(nextPage, 1), totalPages);
    const next = { ...filters, page: safe };
    setFilters(next);
    loadEvents(next);
  }

  async function askQuestion(e) {
    e.preventDefault();
    if (!prompt.trim() || !activeJobId) return;
    setBusyPrompt(true);
    setAssistantError("");
    setAnswer(null);
    try {
      const res = await api.post(`/api/jobs/${activeJobId}/ask`, { prompt });
      setAnswer(res.data);
    } catch (err) {
      const detail = err.response?.data?.detail;
      setAssistantError(typeof detail === "string" ? detail : "The AI query could not be completed.");
    } finally {
      setBusyPrompt(false);
    }
  }

  function goToDashboard(jobId) {
    setActiveJobId(jobId);
    setMetrics(null);
    setEvents(null);
    setAnomalies(null);
    setAnswer(null);
    setAssistantError("");
    setFilters(defaultFilters);
    loadedJobRef.current = null;
    setView("dashboard");
  }

  function goToUpload() {
    setView("upload");
  }

  // ── Render ────────────────────────────────────────────────────────────────
  if (authState === "checking") {
    return (
      <div className="auth-loading">
        <div className="auth-loading-inner">
          <div className="auth-spinner" />
          <p>Loading…</p>
        </div>
      </div>
    );
  }

  if (authState === "unauthenticated") {
    return <AuthPage onAuth={handleAuth} />;
  }

  if (view === "upload") {
    return (
      <UploadPage
        sessions={sessions}
        uploadErrors={uploadErrors}
        onUpload={uploadFiles}
        onViewDashboard={goToDashboard}
      />
    );
  }

  return (
    <DashboardPage
      sessions={sessions}
      activeJobId={activeJobId}
      onChangeJob={goToDashboard}
      onUploadNew={goToUpload}
      currentUser={currentUser}
      onLogout={handleLogout}
      metrics={metrics}
      events={events}
      anomalies={anomalies}
      prompt={prompt}
      setPrompt={setPrompt}
      answer={answer}
      busyPrompt={busyPrompt}
      assistantError={assistantError}
      askQuestion={askQuestion}
      filters={filters}
      updateFilter={updateFilter}
      changePage={changePage}
    />
  );
}

// ─── Auth page ────────────────────────────────────────────────────────────────

/**
 * Login / registration page shown to unauthenticated users.
 * Calls onAuth(user) with the resolved user object on success so the parent
 * can transition to the authenticated view without a page reload.
 */
function AuthPage({ onAuth }) {
  const [tab, setTab] = useState("login");
  const [form, setForm] = useState({ username: "", email: "", password: "" });
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  function field(name) {
    return {
      value: form[name],
      onChange: (e) => setForm((f) => ({ ...f, [name]: e.target.value })),
    };
  }

  async function handleSubmit(e) {
    e.preventDefault();
    setBusy(true);
    setError("");
    try {
      const url = tab === "login" ? "/api/auth/login" : "/api/auth/register";
      const body = tab === "login"
        ? { username: form.username, password: form.password }
        : form;
      const { data } = await api.post(url, body);
      localStorage.setItem("codeace_token", data.access_token);
      const { data: user } = await api.get("/api/auth/me");
      onAuth(user);
    } catch (err) {
      const detail = err.response?.data?.detail;
      setError(typeof detail === "string" ? detail : "Authentication failed. Please try again.");
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="auth-page">
      <div className="auth-hero">
        <p className="eyebrow">Codeace Data Portal</p>
        <h1>Intelligent data onboarding</h1>
        <p className="intro">Upload e-commerce event CSV files, watch processing live, and ask for charts in plain English.</p>
      </div>

      <div className="auth-card-wrap">
        <div className="auth-card">
          <div className="auth-tabs">
            <button
              type="button"
              className={tab === "login" ? "auth-tab auth-tab--active" : "auth-tab"}
              onClick={() => { setTab("login"); setError(""); }}
            >
              Sign in
            </button>
            <button
              type="button"
              className={tab === "register" ? "auth-tab auth-tab--active" : "auth-tab"}
              onClick={() => { setTab("register"); setError(""); }}
            >
              Create account
            </button>
          </div>

          <form className="auth-form" onSubmit={handleSubmit}>
            <label className="auth-label">
              Username
              <input className="auth-input" autoComplete="username" required {...field("username")} />
            </label>
            {tab === "register" && (
              <label className="auth-label">
                Email
                <input className="auth-input" type="email" autoComplete="email" required {...field("email")} />
              </label>
            )}
            <label className="auth-label">
              Password
              <input
                className="auth-input"
                type="password"
                autoComplete={tab === "login" ? "current-password" : "new-password"}
                required
                {...field("password")}
              />
            </label>
            {error && <p className="auth-error">{error}</p>}
            <button className="auth-submit" type="submit" disabled={busy}>
              {busy ? "Please wait…" : tab === "login" ? "Sign in" : "Create account"}
            </button>
          </form>
        </div>
      </div>
    </main>
  );
}

// ─── Settings modal ───────────────────────────────────────────────────────────

/**
 * Account settings overlay: change password, sign out, or permanently delete
 * the account. All destructive actions require the current password to prevent
 * abuse if a session is left open on a shared machine.
 */
function SettingsModal({ currentUser, onClose, onLogout }) {
  const [pwForm, setPwForm] = useState({ current_password: "", new_password: "", confirm: "" });
  const [pwMsg, setPwMsg] = useState(null); // {type: "ok"|"err", text}
  const [pwBusy, setPwBusy] = useState(false);

  const [delPw, setDelPw] = useState("");
  const [delMsg, setDelMsg] = useState("");
  const [delBusy, setDelBusy] = useState(false);

  async function handlePasswordUpdate(e) {
    e.preventDefault();
    if (pwForm.new_password !== pwForm.confirm) {
      setPwMsg({ type: "err", text: "New passwords do not match." });
      return;
    }
    setPwBusy(true);
    setPwMsg(null);
    try {
      const { data } = await api.put("/api/auth/password", {
        current_password: pwForm.current_password,
        new_password: pwForm.new_password,
      });
      setPwMsg({ type: "ok", text: data.message });
      setPwForm({ current_password: "", new_password: "", confirm: "" });
    } catch (err) {
      const detail = err.response?.data?.detail;
      setPwMsg({ type: "err", text: typeof detail === "string" ? detail : "Password update failed." });
    } finally {
      setPwBusy(false);
    }
  }

  async function handleDeleteAccount(e) {
    e.preventDefault();
    setDelBusy(true);
    setDelMsg("");
    try {
      await api.delete("/api/auth/account", { data: { password: delPw } });
      onLogout();
    } catch (err) {
      const detail = err.response?.data?.detail;
      setDelMsg(typeof detail === "string" ? detail : "Account deletion failed.");
      setDelBusy(false);
    }
  }

  return (
    <div className="modal-overlay" onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <div className="modal-card">
        <div className="modal-header">
          <h2>Account Settings</h2>
          <button className="modal-close" onClick={onClose} aria-label="Close">✕</button>
        </div>

        <div className="modal-user-info">
          <span className="modal-username">{currentUser?.username}</span>
          <span className="modal-email">{currentUser?.email}</span>
        </div>

        {/* Change password */}
        <section className="modal-section">
          <h3>Change Password</h3>
          <form onSubmit={handlePasswordUpdate}>
            <label className="auth-label">
              Current password
              <input
                className="auth-input"
                type="password"
                required
                value={pwForm.current_password}
                onChange={(e) => setPwForm((f) => ({ ...f, current_password: e.target.value }))}
              />
            </label>
            <label className="auth-label">
              New password
              <input
                className="auth-input"
                type="password"
                required
                value={pwForm.new_password}
                onChange={(e) => setPwForm((f) => ({ ...f, new_password: e.target.value }))}
              />
            </label>
            <label className="auth-label">
              Confirm new password
              <input
                className="auth-input"
                type="password"
                required
                value={pwForm.confirm}
                onChange={(e) => setPwForm((f) => ({ ...f, confirm: e.target.value }))}
              />
            </label>
            {pwMsg && (
              <p className={pwMsg.type === "ok" ? "msg-ok" : "auth-error"}>{pwMsg.text}</p>
            )}
            <button className="auth-submit" type="submit" disabled={pwBusy}>
              {pwBusy ? "Updating…" : "Update Password"}
            </button>
          </form>
        </section>

        {/* Logout */}
        <section className="modal-section">
          <h3>Session</h3>
          <button className="btn-outline" onClick={onLogout}>Sign out</button>
        </section>

        {/* Delete account */}
        <section className="modal-section modal-danger-zone">
          <h3>Delete Account</h3>
          <p>This permanently removes your account. This action cannot be undone.</p>
          <form onSubmit={handleDeleteAccount}>
            <label className="auth-label">
              Confirm your password
              <input
                className="auth-input"
                type="password"
                required
                value={delPw}
                onChange={(e) => setDelPw(e.target.value)}
              />
            </label>
            {delMsg && <p className="auth-error">{delMsg}</p>}
            <button className="btn-danger" type="submit" disabled={delBusy}>
              {delBusy ? "Deleting…" : "Delete Account"}
            </button>
          </form>
        </section>
      </div>
    </div>
  );
}

// ─── Upload page ──────────────────────────────────────────────────────────────

/**
 * Landing page for authenticated users. Accepts one or more CSV files via
 * drag-and-drop or file picker and shows live upload progress cards.
 * Once at least one job completes, a button appears to open its dashboard.
 */
function UploadPage({ sessions, uploadErrors, onUpload, onViewDashboard }) {
  const completedSessions = sessions.filter((s) => s.status === "completed");

  return (
    <main className="upload-page">
      <div className="upload-hero">
        <p className="eyebrow">Codeace Data Portal</p>
        <h1>Intelligent data onboarding</h1>
        <p className="intro">
          Upload e-commerce event CSV files, watch processing live, then explore the dataset and ask
          for charts in plain English.
        </p>
      </div>

      <section className="upload-section">
        <h2>Upload data</h2>
        <p className="upload-hint">Select one or more CSV files — they are uploaded and processed simultaneously.</p>

        <label className="upload-drop-zone">
          <div className="upload-drop-inner">
            <span className="upload-drop-icon">↑</span>
            <strong>Choose files or drag &amp; drop</strong>
            <small>Accepts .csv · Multiple files supported</small>
          </div>
          <input
            type="file"
            accept=".csv"
            multiple
            onChange={(e) => {
              if (e.target.files?.length) onUpload(e.target.files, e.target);
            }}
          />
        </label>

        {uploadErrors.map((err, i) => (
          <p key={i} className="error upload-error">{err}</p>
        ))}

        {sessions.length > 0 && (
          <div className="session-list">
            {sessions.map((s) => (
              <SessionCard key={s.jobId} session={s} />
            ))}
          </div>
        )}

        {completedSessions.length > 0 && (
          <div className="upload-actions">
            {completedSessions.length === 1 ? (
              <button
                className="btn-view-dashboard"
                onClick={() => onViewDashboard(completedSessions[0].jobId)}
              >
                View Dashboard →
              </button>
            ) : (
              <div className="multi-job-actions">
                <p>Multiple datasets ready — choose one to view:</p>
                {completedSessions.map((s) => (
                  <button key={s.jobId} className="btn-job" onClick={() => onViewDashboard(s.jobId)}>
                    {s.filename}
                  </button>
                ))}
              </div>
            )}
          </div>
        )}
      </section>
    </main>
  );
}

function SessionCard({ session }) {
  const isDone = session.status === "completed";
  const isFailed = session.status === "failed";
  const progress = session.progress || 0;
  return (
    <div className={`session-card${isDone ? " session-card--done" : isFailed ? " session-card--failed" : ""}`}>
      <div className="session-card-top">
        <span className="session-filename">{session.filename}</span>
        <span className={`session-badge session-badge--${session.status}`}>{session.status}</span>
      </div>
      <div className="progress-wrap">
        <div className="progress-bar" style={{ width: `${progress}%` }} />
      </div>
      <div className="session-card-bottom">
        <small>{session.stage || "Waiting…"}</small>
        {session.rows_processed ? (
          <small>{Number(session.rows_processed).toLocaleString()} rows</small>
        ) : null}
        {session.error ? <small className="error">{session.error}</small> : null}
      </div>
    </div>
  );
}

// ─── Dashboard page ───────────────────────────────────────────────────────────

/**
 * Main analytics view. Composes all dashboard sections (Metrics, AI assistant,
 * Anomalies, Events table) and the top navigation bar. When multiple datasets
 * are loaded, a job selector dropdown lets the user switch between them.
 */
function DashboardPage({
  sessions, activeJobId, onChangeJob, onUploadNew, currentUser, onLogout,
  metrics, events, anomalies,
  prompt, setPrompt, answer, busyPrompt, assistantError, askQuestion,
  filters, updateFilter, changePage,
}) {
  const [showSettings, setShowSettings] = useState(false);
  const completedSessions = sessions.filter((s) => s.status === "completed");
  const activeSession = completedSessions.find((s) => s.jobId === activeJobId);

  return (
    <main>
      <div className="dashboard-topbar">
        <div className="dashboard-topbar-left">
          <span className="dashboard-topbar-title">Dashboard</span>
          {completedSessions.length > 1 ? (
            <select
              className="job-selector"
              value={activeJobId || ""}
              onChange={(e) => onChangeJob(e.target.value)}
            >
              {completedSessions.map((s) => (
                <option key={s.jobId} value={s.jobId}>{s.filename}</option>
              ))}
            </select>
          ) : activeSession ? (
            <span className="dashboard-topbar-file">{activeSession.filename}</span>
          ) : null}
        </div>
        <div className="dashboard-topbar-right">
          <button className="btn-upload-new" onClick={onUploadNew}>↑ Upload New Data</button>
          <button
            className="btn-settings"
            onClick={() => setShowSettings(true)}
            aria-label="Account settings"
            title={currentUser?.username}
          >
            ⚙
          </button>
        </div>
      </div>

      <Metrics metrics={metrics} />
      <PromptBar
        prompt={prompt}
        setPrompt={setPrompt}
        askQuestion={askQuestion}
        busyPrompt={busyPrompt}
        error={assistantError}
        answer={answer}
      />
      <AnomalyPanel anomalies={anomalies} />
      <EventsTable
        events={events}
        filters={filters}
        updateFilter={updateFilter}
        changePage={changePage}
      />

      {showSettings && (
        <SettingsModal
          currentUser={currentUser}
          onClose={() => setShowSettings(false)}
          onLogout={onLogout}
        />
      )}
    </main>
  );
}

// ─── Dashboard sections ───────────────────────────────────────────────────────

/** Summary KPI cards and pre-built charts for the active dataset. */
function Metrics({ metrics }) {
  if (!metrics) return null;
  const totals = metrics.totals;
  const averageRevenue = metrics.average_revenue_by_day || [];
  return (
    <section>
      <h2>Dashboard</h2>
      <div className="metric-grid">
        <Metric label="Events" value={totals.total_events} />
        <Metric label="Users" value={totals.users} />
        <Metric label="Sessions" value={totals.sessions} />
        <Metric label="Purchases" value={totals.purchases} />
        <Metric label="Revenue" value={`$${Number(totals.revenue || 0).toLocaleString()}`} />
      </div>
      <div className="chart-grid">
        <AverageRevenueCard rows={averageRevenue} />
        <PlotCard title="Events by type" rows={metrics.by_type} chartType="pie" x="event_type" y="events" />
        <PlotCard title="Top brands by revenue" rows={metrics.top_brands} chartType="bar" x="brand" y="revenue" />
        <PlotCard title="Top categories by revenue" rows={metrics.top_categories} chartType="bar" x="category_code" y="revenue" />
      </div>
    </section>
  );
}

function AverageRevenueCard({ rows }) {
  if (!rows?.length) {
    return (
      <div className="insight-card">
        <span>Average revenue per day of month</span>
        <strong>$0</strong>
        <p>No purchase revenue found.</p>
      </div>
    );
  }
  const latest = rows[rows.length - 1];
  const value = latest.average_daily_revenue ?? latest.average_revenue ?? 0;
  return (
    <div className="insight-card">
      <span>Average revenue per day of month</span>
      <strong>${Number(value || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}</strong>
      <p>
        {latest.event_month}: ${Number(latest.revenue || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })} revenue across {latest.days_in_month || "month"} days
      </p>
    </div>
  );
}

function Metric({ label, value }) {
  return (
    <div className="metric">
      <span>{label}</span>
      <strong>{typeof value === "number" ? value.toLocaleString() : value}</strong>
    </div>
  );
}

function PromptBar({ prompt, setPrompt, askQuestion, busyPrompt, error, answer }) {
  return (
    <section>
      <div className="assistant-heading">
        <h2>AI Analysis assistant</h2>
        <p>Ask for a table, metric, or chart from this uploaded dataset.</p>
      </div>
      <form className="prompt-bar" onSubmit={askQuestion}>
        <input
          value={prompt}
          onChange={(event) => setPrompt(event.target.value)}
          placeholder="What is the revenue for this month?"
        />
        <button disabled={busyPrompt}>{busyPrompt ? "Asking..." : "Ask"}</button>
      </form>
      {error ? <p className="assistant-error">{error}</p> : null}
      {answer ? <AnswerPanel answer={answer} /> : null}
    </section>
  );
}

function AnswerPanel({ answer }) {
  const rows = answer.rows || [];
  const chartType = answer.chart_type || "table";
  const isMetric = chartType === "metric" || (rows.length === 1 && rows[0] && Object.keys(rows[0]).length <= 4);
  const isChart = !isMetric && ["bar", "line", "pie", "scatter"].includes(chartType);
  return (
    <div className="assistant-result">
      <div className="section-heading">
        <h2>{answer.title}</h2>
        <code>{chartType}</code>
      </div>
      {isMetric ? <MetricResult title={answer.title} row={rows[0]} preferredKey={answer.y} /> : null}
      {isChart ? (
        <PlotCard title={answer.title} rows={rows} chartType={chartType} x={answer.x} y={answer.y} />
      ) : null}
      <div className="sql-block">
        <strong>Generated SQL</strong>
        <pre>{answer.sql || "No SQL returned."}</pre>
      </div>
      <DataTable rows={rows} columns={answer.columns} />
    </div>
  );
}

/**
 * Renders a Plotly chart for a given dataset. Supports bar, line, pie, scatter,
 * and multi_line chart types. Falls back to a DataTable when chart data is
 * insufficient or the chart type is 'table'/'metric'.
 *
 * fallbackY is tried when the primary y key has no non-null values, which can
 * happen when the AI picks a column name that doesn't match the query result.
 */
function PlotCard({ title, rows, chartType, x, y, series, fallbackY }) {
  const plotData = useMemo(() => {
    if (!rows?.length || chartType === "table" || chartType === "metric" || !x || !y) return [];
    const yKey = rows.some((row) => row[y] !== undefined && row[y] !== null) ? y : fallbackY;
    if (!yKey || !rows.some((row) => row[yKey] !== undefined && row[yKey] !== null)) return [];
    if (chartType === "multi_line" && series) {
      const groups = rows.reduce((acc, row) => {
        const key = row[series] || "Series";
        acc[key] = acc[key] || [];
        acc[key].push(row);
        return acc;
      }, {});
      return Object.entries(groups).map(([name, groupRows]) => ({
        type: "scatter",
        mode: "lines+markers",
        name,
        x: groupRows.map((row) => row[x]),
        y: groupRows.map((row) => row[yKey]),
      }));
    }
    const labels = rows.map((row) => row[x]);
    const values = rows.map((row) => row[yKey]);
    if (chartType === "pie") {
      return [{
        type: "pie",
        labels,
        values,
        hole: 0.35,
        textinfo: "label+percent",
        textposition: "outside",
        automargin: true,
        marker: { line: { color: "#ffffff", width: 2 } },
        textfont: { size: 14, color: "#18201f", family: "Inter, Arial, sans-serif" },
      }];
    }
    if (chartType === "scatter") return [{ type: "scatter", mode: "markers", x: labels, y: values }];
    return [{
      type: chartType === "line" ? "scatter" : "bar",
      mode: chartType === "line" ? "lines+markers" : undefined,
      x: labels,
      y: values,
      marker: { color: "#0f766e" },
      text: chartType === "bar" ? values.map((value) => formatCell(value)) : undefined,
      textposition: chartType === "bar" ? "auto" : undefined,
      textfont: chartType === "bar" ? { size: 12, color: "#18201f", family: "Inter, Arial, sans-serif" } : undefined,
    }];
  }, [rows, chartType, x, y, series, fallbackY]);

  if (!rows?.length) return <div className="plot-card">No rows returned.</div>;
  if (chartType === "metric") return <MetricResult title={title} row={rows[0]} preferredKey={y} />;
  if (!plotData.length) {
    return (
      <div className="plot-card plot-fallback">
        <h3>{title}</h3>
        <DataTable rows={rows} />
      </div>
    );
  }
  const chartLayout = {
    autosize: true,
    margin: chartType === "pie" ? { t: 20, r: 140, b: 40, l: 96 } : { t: 20, r: 28, b: 122, l: 82 },
    paper_bgcolor: "transparent",
    plot_bgcolor: "transparent",
    font: { size: 14, color: "#18201f", family: "Inter, Arial, sans-serif" },
  };
  if (chartType === "bar") chartLayout.xaxis = { automargin: true, tickangle: -35, tickfont: { size: 12, color: "#18201f" } };
  if (chartType !== "pie") chartLayout.yaxis = { automargin: true, tickfont: { size: 12, color: "#18201f" } };
  if (chartType === "pie") {
    chartLayout.showlegend = true;
    chartLayout.legend = { font: { size: 14, color: "#18201f" }, x: 1.02, y: 0.95 };
  }
  if (chartType === "multi_line") {
    chartLayout.xaxis = { title: "Day of month", dtick: 5 };
    chartLayout.legend = { orientation: "h", y: -0.25 };
  }
  return (
    <div className="plot-card">
      <h3 className="chart-title">{title}</h3>
      <PlotlyChart data={plotData} layout={chartLayout} config={{ responsive: true, displayModeBar: false }} />
    </div>
  );
}

/**
 * Imperative Plotly wrapper. Uses Plotly.react (diff-based update) rather than
 * Plotly.newPlot so chart re-renders from data changes are efficient. Purges
 * the chart on unmount to avoid memory leaks from detached DOM nodes.
 */
function PlotlyChart({ data, layout, config }) {
  const ref = useRef(null);
  const [renderError, setRenderError] = useState("");

  useEffect(() => {
    if (!ref.current) return undefined;
    let alive = true;
    setRenderError("");
    try {
      Plotly.react(ref.current, data, cleanPlotlyObject(layout), cleanPlotlyObject(config)).catch((error) => {
        if (alive) setRenderError(error.message || "Chart render failed.");
      });
    } catch (error) {
      setRenderError(error.message || "Chart render failed.");
    }
    const resize = () => { if (ref.current) Plotly.Plots.resize(ref.current); };
    window.addEventListener("resize", resize);
    return () => {
      alive = false;
      window.removeEventListener("resize", resize);
      if (ref.current) Plotly.purge(ref.current);
    };
  }, [data, layout, config]);

  return (
    <>
      <div className="plotly-target" ref={ref} />
      {renderError ? <p className="chart-error">{renderError}</p> : null}
    </>
  );
}

// Plotly throws if layout/config contain undefined or null values, so strip them.
function cleanPlotlyObject(value) {
  if (Array.isArray(value)) return value.map(cleanPlotlyObject);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([, v]) => v !== undefined && v !== null)
        .map(([k, v]) => [k, cleanPlotlyObject(v)])
    );
  }
  return value;
}

function MetricResult({ title, row, preferredKey }) {
  const entries = Object.entries(row || {});
  const primaryKey = preferredKey && row?.[preferredKey] !== undefined ? preferredKey : entries[0]?.[0];
  const secondaryEntries = entries.filter(([key]) => key !== primaryKey);
  return (
    <div className="metric-result">
      <span>{title}</span>
      <strong>{formatCell(row?.[primaryKey])}</strong>
      <div className="metric-detail-grid">
        {secondaryEntries.map(([key, value]) => (
          <small key={key}>{key}: {formatCell(value)}</small>
        ))}
      </div>
    </div>
  );
}

function AnomalyPanel({ anomalies }) {
  if (!anomalies) return null;
  return (
    <section>
      <div className="anomaly-box">
        <div className="anomaly-box-header">
          <h2>Anomalies</h2>
          <code>{anomalies.method}</code>
        </div>
        <div className="anomaly-grid">
          {anomalies.columns?.map((item) => (
            <div className="anomaly" key={item.column}>
              <span>No. of anomalies found in {labelForColumn(item.column)} column</span>
              <strong>{item.anomaly_count.toLocaleString()}</strong>
              <small>outside {Number(item.lower_bound).toFixed(2)} to {Number(item.upper_bound).toFixed(2)}</small>
            </div>
          ))}
        </div>
        {anomalies.report ? (
          <div className="anomaly-findings">
            <h3 className="anomaly-findings-heading">Anomaly Findings</h3>
            <ReportText text={anomalies.report} />
          </div>
        ) : null}
        {anomalies.columns?.map((item) =>
          item.sample_rows?.length ? (
            <details className="anomaly-samples" key={`${item.column}-samples`}>
              <summary>Flagged {labelForColumn(item.column)} rows</summary>
              <DataTable rows={item.sample_rows} />
            </details>
          ) : null
        )}
      </div>
    </section>
  );
}

/**
 * Renders the Ollama-generated anomaly report as structured HTML.
 * Strips markdown syntax (##, **, numbered lists, dashes) that the model may
 * include despite being told to use plain text, promoting likely headings to
 * <h3> and everything else to <p>.
 */
function ReportText({ text }) {
  const lines = String(text || "").split("\n").map((line) => line.trim()).filter(Boolean);
  if (!lines.length) return null;
  return (
    <div className="report">
      {lines.map((line, index) => {
        const cleaned = line.replace(/^#+\s*/, "").replace(/^\d+\.\s*/, "").replace(/\*\*/g, "").replace(/^-+\s*/, "");
        const looksLikeHeading = /^#{1,3}\s/.test(line) || /^\d+\.\s/.test(line) || /\:$/.test(cleaned);
        return looksLikeHeading ? <h3 key={index}>{cleaned.replace(/\:$/, "")}</h3> : <p key={index}>{cleaned}</p>;
      })}
    </div>
  );
}

function EventsTable({ events, filters, updateFilter, changePage }) {
  if (!events) return null;
  const dateRange = events.date_range || {};
  const minDate = toDateInputValue(dateRange.min_event_time);
  const maxDate = toDateInputValue(dateRange.max_event_time);
  const brandOptions = events.filter_options?.brands || [];
  const categoryOptions = events.filter_options?.categories || [];
  return (
    <section>
      <h2>Data explorer</h2>
      <p>{events.total.toLocaleString()} matching rows</p>
      <div className="filter-grid">
        <label>
          Event type
          <select value={filters.event_type} onChange={(e) => updateFilter("event_type", e.target.value)}>
            <option value="">All</option>
            <option value="view">view</option>
            <option value="cart">cart</option>
            <option value="purchase">purchase</option>
          </select>
        </label>
        <label>
          Brand
          <select value={filters.brand} onChange={(e) => updateFilter("brand", e.target.value)}>
            <option value="">All</option>
            {brandOptions.map((b) => <option key={b} value={b}>{b}</option>)}
          </select>
        </label>
        <label>
          Category
          <select value={filters.category} onChange={(e) => updateFilter("category", e.target.value)}>
            <option value="">All</option>
            {categoryOptions.map((c) => <option key={c} value={c}>{c}</option>)}
          </select>
        </label>
        <label>
          Min price
          <input type="number" min="0" value={filters.min_price} onChange={(e) => updateFilter("min_price", e.target.value)} />
        </label>
        <label>
          Max price
          <input type="number" min="0" value={filters.max_price} onChange={(e) => updateFilter("max_price", e.target.value)} />
        </label>
        <label>
          Start date
          <input type="date" value={filters.start_time || minDate} onChange={(e) => updateFilter("start_time", e.target.value)} />
        </label>
        <label>
          End date
          <input type="date" value={filters.end_time || maxDate} onChange={(e) => updateFilter("end_time", e.target.value)} />
        </label>
        <label>
          Rows
          <select value={filters.page_size} onChange={(e) => updateFilter("page_size", Number(e.target.value))}>
            <option value="10">10</option>
            <option value="25">25</option>
            <option value="50">50</option>
            <option value="100">100</option>
          </select>
        </label>
      </div>
      {minDate && maxDate ? <p className="date-range-note">Dataset range: {formatCell(dateRange.min_event_time)} to {formatCell(dateRange.max_event_time)}</p> : null}
      <DataTable rows={events.rows} />
      <div className="pagination">
        <button disabled={events.page <= 1} onClick={() => changePage(events.page - 1)}>Previous</button>
        <span>Page {events.page} of {events.total_pages}</span>
        <button disabled={events.page >= events.total_pages} onClick={() => changePage(events.page + 1)}>Next</button>
      </div>
    </section>
  );
}

function DataTable({ rows, columns }) {
  const cols = columns || Object.keys(rows?.[0] || {});
  if (!rows?.length) return <p>No data to display.</p>;
  return (
    <div className="table-wrap">
      <table>
        <thead><tr>{cols.map((c) => <th key={c}>{c}</th>)}</tr></thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i}>{cols.map((c) => <td key={c}>{formatCell(row[c])}</td>)}</tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function formatCell(value) {
  if (value === null || value === undefined) return "";
  if (typeof value === "number") return Number.isInteger(value) ? value.toLocaleString() : value.toLocaleString(undefined, { maximumFractionDigits: 2 });
  return String(value);
}

function toDateInputValue(value) {
  if (!value) return "";
  return String(value).slice(0, 10);
}

function labelForColumn(column) {
  if (column === "price") return "Price";
  return String(column || "").replace(/_/g, " ");
}
