import React from "react";
import { useEffect, useMemo, useState } from "react"
import { Link, useNavigate } from "react-router-dom"
import Nav from "../components/Nav"
import SparklesBg from "../components/SparklesBg"
import { authFetch, getAuthHeaders } from "../lib/auth"
import { apiUrl } from "../lib/api"

const STAGE_COPY = {
  queued: "Preparing search",
  discovering_companies: "Finding matching companies",
  discovering_contacts: "Identifying decision makers",
  scoring_results: "Generating and verifying email candidates",
  completed: "Search complete",
  failed: "Search failed",
}

function stageProgress(stage) {
  const order = ["queued", "discovering_companies", "discovering_contacts", "scoring_results", "completed"]
  const index = Math.max(0, order.indexOf(stage))
  return Math.round((index / (order.length - 1)) * 100)
}

export default function EnterpriseIntelligencePage({ user, onLogout }) {
  const navigate = useNavigate()
  const [niche, setNiche] = useState("")
  const [location, setLocation] = useState("")
  const [companySize, setCompanySize] = useState("")
  const [limit, setLimit] = useState(20)
  const [jobId, setJobId] = useState("")
  const [job, setJob] = useState(null)
  const [results, setResults] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState("")
  const [csvBusy, setCsvBusy] = useState(false)
  const canUseEnterprise = (user?.role || "").toLowerCase() === "admin" || Boolean(user?.enterprise_access)

  const currentStage = STAGE_COPY[job?.stage] || "Ready"
  const progress = stageProgress(job?.stage || "queued")

  useEffect(() => {
    if (!jobId) return
    let cancelled = false
    const poll = async () => {
      const res = await authFetch(`/enterprise/search/${jobId}`, {}, () => navigate("/login"))
      const data = await res.json().catch(() => ({}))
      if (cancelled) return
      if (!res.ok) {
        setError(data?.message || data?.detail || "Unable to load enterprise search status.")
        return
      }
      const nextJob = data?.job || data?.data?.job || null
      setJob(nextJob)
      if (nextJob?.status === "completed") {
        const resultsRes = await authFetch(`/enterprise/search/${jobId}/results`, {}, () => navigate("/login"))
        const resultsData = await resultsRes.json().catch(() => ({}))
        if (!cancelled && resultsRes.ok) {
          setResults(resultsData?.results || resultsData?.data?.results || [])
          setLoading(false)
        }
        return
      }
      if (nextJob?.status === "failed") {
        setLoading(false)
        setError(nextJob?.error || "Enterprise search failed.")
        return
      }
      window.setTimeout(poll, 2000)
    }
    poll()
    return () => {
      cancelled = true
    }
  }, [jobId, navigate])

  const startSearch = async () => {
    setLoading(true)
    setError("")
    setResults([])
    setJob(null)
    const res = await authFetch("/enterprise/search", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        niche,
        location,
        company_size: companySize,
        limit,
      }),
    }, () => navigate("/login"))
    const data = await res.json().catch(() => ({}))
    if (!res.ok) {
      setLoading(false)
      setError(data?.message || data?.detail || "We couldn't start this enterprise search.")
      return
    }
    setJobId(data?.job_id || data?.data?.job_id || "")
    setJob({
      status: data?.status || data?.data?.status || "queued",
      stage: data?.stage || data?.data?.stage || "queued",
      progress_message: "Preparing enterprise search",
    })
  }

  const exportCsv = async () => {
    if (!jobId || !results.length || csvBusy) return
    setCsvBusy(true)
    setError("")
    try {
      const response = await fetch(apiUrl(`/enterprise/search/${jobId}/download`), {
        method: "GET",
        headers: getAuthHeaders({}),
      })
      if (!response.ok) {
        const payload = await response.json().catch(() => ({}))
        throw new Error(payload?.message || payload?.detail || "CSV export is not ready.")
      }
      const blob = await response.blob()
      const url = URL.createObjectURL(blob)
      const anchor = document.createElement("a")
      anchor.href = url
      anchor.download = `leadscout_enterprise_${jobId.slice(0, 8)}.csv`
      anchor.click()
      URL.revokeObjectURL(url)
    } catch (err) {
      setError(String(err?.message || "CSV export is not ready."))
    } finally {
      setCsvBusy(false)
    }
  }

  const topResults = useMemo(() => results.slice(0, 100), [results])

  return (
    <div className="page">
      <SparklesBg />
      <Nav user={user} onLogout={onLogout} />
      <div style={{ paddingTop: 64 }}>
        <div style={{ maxWidth: 1240, margin: "0 auto", padding: "36px 24px 72px" }}>
          {!canUseEnterprise ? (
            <div className="card" style={{ textAlign: "center" }}>
              <span className="badge badge-red" style={{ marginBottom: 12, display: "inline-block" }}>Restricted</span>
              <h1 style={{ fontSize: 28, fontWeight: 800, marginBottom: 12 }}>Enterprise Intelligence Module</h1>
              <p style={{ color: "var(--text-secondary)", lineHeight: 1.7, marginBottom: 16 }}>
                This module is available only to admin users and accounts explicitly approved by admin.
              </p>
              <Link to="/dashboard" className="btn btn-ghost">Back to dashboard</Link>
            </div>
          ) : (
            <>
              <div className="card" style={{ marginBottom: 18 }}>
                <div style={{ display: "flex", justifyContent: "space-between", gap: 16, alignItems: "start", flexWrap: "wrap" }}>
                  <div>
                    <span className="badge badge-cyan" style={{ marginBottom: 12, display: "inline-block" }}>Enterprise</span>
                    <h1 style={{ fontSize: 30, fontWeight: 800, letterSpacing: "-0.03em", marginBottom: 8 }}>Lead intelligence</h1>
                    <p style={{ color: "var(--text-secondary)", lineHeight: 1.7, maxWidth: 760 }}>
                      Discover companies, extract likely decision makers from public pages, generate probable email patterns, and rank the strongest outreach paths.
                    </p>
                  </div>
                  <div style={{ minWidth: 240, padding: 16, border: "1px solid var(--border)", borderRadius: 18, background: "rgba(255,255,255,0.03)" }}>
                    <div style={{ fontSize: 11, letterSpacing: "0.08em", textTransform: "uppercase", color: "var(--text-muted)", marginBottom: 6 }}>Compliance note</div>
                    <div style={{ color: "var(--text-secondary)", fontSize: 13, lineHeight: 1.6 }}>
                      Uses publicly accessible web data only. Avoids logged-in private networks and should be reviewed before outreach.
                    </div>
                  </div>
                </div>

                <div style={{ display: "grid", gridTemplateColumns: "2fr 1.2fr 1fr 0.8fr auto", gap: 12, marginTop: 20 }} className="leads-filters-grid">
                  <input value={niche} onChange={(e) => setNiche(e.target.value)} placeholder="Niche or search phrase, e.g. dentists in Mumbai" />
                  <input value={location} onChange={(e) => setLocation(e.target.value)} placeholder="Optional location filter" />
                  <select value={companySize} onChange={(e) => setCompanySize(e.target.value)}>
                    <option value="">Any company size</option>
                    <option value="small">Small business</option>
                    <option value="mid-market">Mid-market</option>
                    <option value="large">Large company</option>
                  </select>
                  <input
                    type="number"
                    min="5"
                    max="50"
                    value={limit}
                    onChange={(e) => setLimit(Math.max(5, Math.min(50, Number(e.target.value || 20))))}
                    placeholder="Limit"
                  />
                  <button className="btn btn-primary" onClick={startSearch} disabled={loading || !niche.trim()}>
                    {loading ? "Running..." : "Run Search"}
                  </button>
                </div>
              </div>

              <div className="card" style={{ marginBottom: 18, overflow: "hidden" }}>
                <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center", marginBottom: 14, flexWrap: "wrap" }}>
                  <div>
                    <div style={{ fontSize: 11, letterSpacing: "0.08em", textTransform: "uppercase", color: "var(--text-muted)", marginBottom: 6 }}>Progress</div>
                    <div style={{ fontSize: 24, fontWeight: 800 }}>{currentStage}</div>
                    <div style={{ color: "var(--text-secondary)", marginTop: 6 }}>
                      {job?.progress_message || "Ready to start an enterprise search."}
                    </div>
                  </div>
                  <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
                    {jobId && <span className="badge badge-cyan">Job {jobId.slice(0, 8)}</span>}
                    <span className="badge badge-gold">{results.length || job?.lead_count || 0} results</span>
                    <button className="btn btn-ghost" onClick={exportCsv} disabled={!results.length || csvBusy}>
                      {csvBusy ? "Preparing..." : "Export CSV"}
                    </button>
                  </div>
                </div>
                <div style={{ position: "relative", height: 12, borderRadius: 999, background: "rgba(255,255,255,0.08)", overflow: "hidden" }}>
                  <div
                    style={{
                      width: `${progress}%`,
                      height: "100%",
                      borderRadius: 999,
                      background: "linear-gradient(90deg, rgba(50,198,255,0.7), rgba(136,237,255,0.95))",
                      boxShadow: "0 0 24px rgba(50,198,255,0.35)",
                      transition: "width 500ms ease",
                    }}
                  />
                </div>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: 12, marginTop: 16 }}>
                  {["Finding matching companies", "Identifying decision makers", "Generating email candidates", "Ranking final results"].map((label, index) => {
                    const activeIndex = Math.max(0, ["queued", "discovering_companies", "discovering_contacts", "scoring_results", "completed"].indexOf(job?.stage || "queued"))
                    const complete = index < activeIndex
                    const active = index === activeIndex
                    return (
                      <div
                        key={label}
                        style={{
                          padding: 14,
                          borderRadius: 16,
                          border: active ? "1px solid rgba(50,198,255,0.5)" : "1px solid var(--border)",
                          background: complete
                            ? "linear-gradient(180deg, rgba(50,198,255,0.14), rgba(255,255,255,0.03))"
                            : active
                              ? "linear-gradient(180deg, rgba(50,198,255,0.10), rgba(255,255,255,0.03))"
                              : "rgba(255,255,255,0.02)",
                          boxShadow: active ? "0 0 28px rgba(50,198,255,0.15)" : "none",
                        }}
                      >
                        <div style={{ fontSize: 12, color: complete || active ? "var(--text-primary)" : "var(--text-muted)", lineHeight: 1.5 }}>{label}</div>
                      </div>
                    )
                  })}
                </div>
                {error && <div style={{ marginTop: 16, color: "var(--accent-red)" }}>{error}</div>}
              </div>

              <div className="card" style={{ padding: 0, overflow: "hidden" }}>
                <div style={{ padding: "16px 20px", borderBottom: "1px solid var(--border)", display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center" }}>
                  <div>
                    <div style={{ fontSize: 13, fontWeight: 700 }}>Ranked decision-maker leads</div>
                    <div style={{ fontSize: 12, color: "var(--text-muted)", marginTop: 4 }}>
                      Company, likely decision maker, probable email, confidence score, and source evidence.
                    </div>
                  </div>
                  <div style={{ fontSize: 12, color: "var(--text-muted)" }}>{topResults.length} rows</div>
                </div>
                <div className="table-wrap">
                  {!topResults.length ? (
                    <div style={{ padding: 40, textAlign: "center", color: "var(--text-muted)" }}>
                      {loading ? "Enterprise search in progress..." : "Run a search to generate enterprise leads."}
                    </div>
                  ) : (
                    <table className="data-table">
                      <thead>
                        <tr>
                          <th>Company</th>
                          <th>Decision Maker</th>
                          <th>Role</th>
                          <th>Email</th>
                          <th>Confidence</th>
                          <th>Source</th>
                        </tr>
                      </thead>
                      <tbody>
                        {topResults.map((item) => (
                          <tr key={`${item.id || item.domain}-${item.email || item.company_name}`}>
                            <td>
                              <div style={{ fontWeight: 600 }}>{item.company_name || "—"}</div>
                              <div style={{ color: "var(--text-muted)", fontSize: 12 }}>{item.domain || "—"}</div>
                            </td>
                            <td>{item.decision_maker_name || "Not identified"}</td>
                            <td>{item.role || "—"}</td>
                            <td>{item.email || "—"}</td>
                            <td>{item.confidence_score || 0}%</td>
                            <td>{item.source || "web"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  )
}
