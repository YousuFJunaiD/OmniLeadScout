import React from "react";
import { useEffect, useMemo, useRef, useState } from "react"
import { useNavigate } from "react-router-dom"
import Nav from "../components/Nav"
import SparklesBg from "../components/SparklesBg"
import { authFetch } from "../lib/auth"


export default function ProfilePage({ user, onLogout }) {
  const navigate = useNavigate()
  const ACTIVE_JOB_KEY = `ls_active_job_${user.id}`
  const [history, setHistory] = useState([])
  const [loading, setLoading] = useState(true)
  const [workingJobId, setWorkingJobId] = useState(null)
  const [downloadingAll, setDownloadingAll] = useState(false)
  const [viewingJob, setViewingJob] = useState(null)
  const [viewingLeads, setViewingLeads] = useState([])
  const [viewingLoading, setViewingLoading] = useState(false)
  const pollingBusyRef = useRef(false)
  const authRequest = (url, options = {}) => authFetch(url, options, () => navigate("/login"))

  const getTotalAreas = (job) => {
    const fromField = Number(job.total_areas || 0)
    if (fromField > 0) return fromField
    try {
      const parsed = JSON.parse(job.areas || "[]")
      return Array.isArray(parsed) ? parsed.length : 0
    } catch {
      return 0
    }
  }

  const canResumeFromStopped = (job) => {
    if (job.status !== "stopped") return false
    const total = getTotalAreas(job)
    const processed = Number(job.processed_areas || 0)
    return total > processed
  }

  const withEffectiveCounts = (jobs) =>
    jobs.map((job) => ({
      ...job,
      effective_lead_count: Number(job.effective_lead_count ?? job.lead_count ?? 0),
      persisted_leads: Number(job.persisted_leads ?? 0),
    }))

  const extractAreaFromQuery = (query, profession) => {
    if (!query) return ""
    const marker = `${profession || ""} in `
    if (profession && query.startsWith(marker)) {
      return query.slice(marker.length).trim()
    }
    const idx = query.toLowerCase().indexOf(" in ")
    if (idx >= 0) return query.slice(idx + 4).trim()
    return ""
  }

  const mergeContinuationRows = (jobs) => {
    const grouped = new Map()
    for (const job of jobs) {
      const key = `${job.niche || ""}|${job.location || ""}`
      const arr = grouped.get(key) || []
      arr.push(job)
      grouped.set(key, arr)
    }

    const merged = []
    for (const group of grouped.values()) {
      const running = group.find(g => g.status === "running" || g.status === "stopping")
      if (!running || group.length === 1) {
        merged.push(...group)
        continue
      }

      const totalLeads = group.reduce((sum, g) => sum + Number(g.effective_lead_count || 0), 0)
      merged.push({
        ...running,
        effective_lead_count: totalLeads,
        merged_job_ids: group.map(g => g.job_id),
        merged_count: group.length,
      })
    }

    return merged.sort((a, b) => {
      const ad = a.created_at ? new Date(a.created_at).getTime() : 0
      const bd = b.created_at ? new Date(b.created_at).getTime() : 0
      return bd - ad
    })
  }

  const refreshHistory = async ({ firstLoad = false } = {}) => {
    if (pollingBusyRef.current) return
    pollingBusyRef.current = true
    if (firstLoad) setLoading(true)
    try {
      const baseRes = await authRequest(`/api/user/history`)
      const baseData = await baseRes.json()
      let jobs = withEffectiveCounts(baseData.history || [])

      const runningJobs = jobs.filter(j => j.status === "running" || j.status === "stopping")
      if (runningJobs.length) {
        const liveStatuses = await Promise.all(
          runningJobs.map(async (job) => {
            try {
              const r = await authRequest(`/api/scrape/status/${job.job_id}`)
              if (!r.ok) return null
              const status = await r.json()
              return { job_id: job.job_id, status }
            } catch {
              return null
            }
          })
        )

        const statusById = {}
        for (const item of liveStatuses) {
          if (item?.job_id && item.status) statusById[item.job_id] = item.status
        }

        jobs = jobs.map((job) => {
          const s = statusById[job.job_id]
          if (!s) return job
          const liveArea = extractAreaFromQuery(s.current_query, job.profession)
          return {
            ...job,
            status: s.status || job.status,
            lead_count: Number(s.lead_count ?? job.lead_count ?? 0),
            effective_lead_count: Number(s.lead_count ?? job.effective_lead_count ?? 0),
            processed_areas: Number(s.processed_areas ?? job.processed_areas ?? 0),
            total_areas: Number(s.total_areas ?? job.total_areas ?? 0),
            location: liveArea || job.location,
          }
        })
      }

      setHistory(mergeContinuationRows(jobs))
    } catch {
    } finally {
      if (firstLoad) setLoading(false)
      pollingBusyRef.current = false
    }
  }

  const deleteJob = async (targetJobId) => {
    if (!window.confirm("Delete this scrape and all its leads? This cannot be undone.")) return
    try {
      await authRequest(`/api/scrape/job/${targetJobId}`, {
        method: "DELETE",
        headers: { "x-user-id": user.id },
      })
      refreshHistory()
    } catch {}
  }

  useEffect(() => {
    refreshHistory({ firstLoad: true })
    const id = setInterval(() => refreshHistory(), 30000)
    return () => clearInterval(id)
  }, [user.id])

  const openJobDashboard = (jobId) => {
    localStorage.setItem(ACTIVE_JOB_KEY, jobId)
    navigate("/dashboard")
  }

  const resumeFromRow = async (job, restartFromBeginning = false) => {
    setWorkingJobId(job.job_id)
    try {
      const res = await authRequest(`/api/scrape/resume/${job.job_id}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          user_id: user.id,
          restart_from_beginning: restartFromBeginning,
          niche: restartFromBeginning ? `${job.profession || "custom"}_${job.location || "custom"}`.replace(/ /g, "_").toLowerCase() : undefined,
        }),
      })
      if (!res.ok) return
      const payload = await res.json()
      if (payload?.job_id) {
        localStorage.setItem(ACTIVE_JOB_KEY, payload.job_id)
      }
      await refreshHistory()
      navigate("/dashboard")
    } finally {
      setWorkingJobId(null)
    }
  }

  const download = async (jobId, filename) => {
    const res  = await authRequest(`/api/scrape/download/${jobId}`)
    const blob = await res.blob()
    const url  = URL.createObjectURL(blob)
    const a    = document.createElement("a")
    a.href = url; a.download = filename; a.click()
    URL.revokeObjectURL(url)
  }

  const downloadAll = async () => {
    setDownloadingAll(true)
    try {
      const res = await authRequest(`/api/scrape/download/all/${user.id}`)
      if (!res.ok) return
      const blob = await res.blob()
      const url = URL.createObjectURL(blob)
      const a = document.createElement("a")
      a.href = url
      a.download = `all_scraped_${user.id}.csv`
      a.click()
      URL.revokeObjectURL(url)
    } finally {
      setDownloadingAll(false)
    }
  }

  const totalLeads = useMemo(
    () => history.reduce((s, h) => s + Number(h.effective_lead_count || 0), 0),
    [history]
  )

  const viewLeads = async (job) => {
    setViewingJob(job)
    setViewingLoading(true)
    setViewingLeads([])
    
    // If it's a merged job, we should ideally fetch all merged_job_ids, but for now we fetch the root one or pass multiple.
    // The endpoint currently takes one string. We can pass comma-separated if we update backend, 
    // but right now it only supports one. Let's just fetch the root job's leads.
    const idsToFetch = job.merged_job_ids ? job.merged_job_ids[0] : job.job_id
    
    try {
      const res = await authRequest(`/api/scrape/job/${idsToFetch}/leads`, {
        headers: { "x-user-id": user.id }
      })
      if (!res.ok) throw new Error("Failed")
      const data = await res.json()
      setViewingLeads(data.leads || [])
    } catch {
      setViewingLeads([])
    } finally {
      setViewingLoading(false)
    }
  }

  return (
    <div className="page">
      <SparklesBg />
      <Nav user={user} onLogout={onLogout} />
      <div style={{ paddingTop: 64 }}>
        <div style={{ maxWidth: 1000, margin: "0 auto", padding: "40px 24px" }}>

          <div className="card card-glow stagger" style={{ marginBottom: 24, display: "flex", alignItems: "center", gap: 24 }}>
            <div style={{ width: 64, height: 64, borderRadius: "50%", background: "linear-gradient(135deg,var(--accent-cyan),var(--accent-violet))", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 24, fontWeight: 800, color: "#020305", flexShrink: 0 }}>
              {user.name?.[0]?.toUpperCase()}
            </div>
            <div style={{ flex: 1 }}>
              <h1 style={{ fontSize: 22, fontWeight: 800, letterSpacing: "-0.02em", marginBottom: 4 }}>{user.name}</h1>
              <p style={{ fontSize: 13, color: "var(--text-secondary)" }}>{user.email}</p>
            </div>
            <div style={{ display: "flex", gap: 24, textAlign: "right" }}>
              {[["Scrapes", history.length, "var(--accent-cyan)"], ["Total Leads", totalLeads.toLocaleString(), "var(--accent-violet)"], ["Completed", history.filter(h => h.status === "completed").length, "var(--accent-gold)"]].map(([l, v, c], i) => (
                <div key={i}>
                  <div style={{ fontSize: 28, fontWeight: 800, color: c, fontFamily: "var(--font-mono)" }}>{v}</div>
                  <div style={{ fontSize: 10, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.08em" }}>{l}</div>
                </div>
              ))}
            </div>
          </div>

          <div className="card">
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
              <p style={{ fontSize: 10, letterSpacing: "0.1em", textTransform: "uppercase", color: "var(--text-muted)", marginBottom: 0 }}>Scrape History</p>
              <div style={{ display: "flex", gap: 8 }}>
                <button className="btn btn-ghost" style={{ padding: "6px 12px", fontSize: 11 }} onClick={() => navigate("/dashboard")}>Start Fresh Scrape</button>
                <button className="btn btn-ghost" style={{ padding: "6px 12px", fontSize: 11 }} onClick={downloadAll} disabled={downloadingAll}>
                  {downloadingAll ? "Preparing..." : "Download All Scraped CSV"}
                </button>
              </div>
            </div>
            {loading ? (
              <div style={{ textAlign: "center", padding: 40, color: "var(--text-muted)" }}>Loading...</div>
            ) : history.length === 0 ? (
              <div style={{ textAlign: "center", padding: 60 }}>
                <div style={{ fontSize: 40, opacity: 0.15, marginBottom: 12 }}>◈</div>
                <p style={{ color: "var(--text-muted)", fontSize: 13 }}>No scrapes yet. Launch your first from the dashboard.</p>
              </div>
            ) : (
              <table className="data-table">
                <thead><tr><th>Job</th><th>Profession</th><th>Location</th><th>Leads</th><th>Date</th><th>Status</th><th></th></tr></thead>
                <tbody>
                  {history.map((h, i) => (
                    <tr key={i}>
                      <td style={{ fontFamily: "var(--font-mono)", fontSize: 11, color: "var(--text-muted)" }}>#{h.job_id?.slice(0,8)}</td>
                      <td>{h.profession || "—"}</td>
                      <td>{h.location || "—"}</td>
                      <td style={{ color: "var(--accent-cyan)", fontFamily: "var(--font-mono)", fontWeight: 600 }}>{Number(h.effective_lead_count||0).toLocaleString()}</td>
                      <td style={{ fontSize: 11 }}>{h.created_at ? new Date(h.created_at).toLocaleDateString() : "—"}</td>
                      <td><span className={`badge ${h.status==="completed"?"badge-green":h.status==="running"?"badge-cyan":"badge-red"}`}>{h.status}</span></td>
                      <td>
                        <div style={{ display: "flex", gap: 8, justifyContent: "flex-end", flexWrap: "wrap" }}>
                          {h.merged_count > 1 && (
                            <span style={{ fontSize: 10, color: "var(--text-muted)", whiteSpace: "nowrap" }}>
                              merged {h.merged_count} runs
                            </span>
                          )}
                          {(h.status === "running" || h.status === "stopping") && (
                            <span style={{ fontSize: 10, color: "var(--text-muted)", whiteSpace: "nowrap" }}>
                              live {Number(h.processed_areas || 0)}/{Number(getTotalAreas(h) || 0)} areas
                            </span>
                          )}
                          {((h.status==="completed" || h.status==="stopped") && Number(h.effective_lead_count||0) > 0) && (
                            <>
                              <button className="btn btn-ghost" style={{ padding:"4px 12px",fontSize:11 }} onClick={() => viewLeads(h)}>👁 View Data</button>
                              <button className="btn btn-ghost" style={{ padding:"4px 12px",fontSize:11 }} onClick={() => download(h.job_id,`${h.profession}_${h.job_id?.slice(0,8)}.csv`)}>↓ CSV</button>
                              <button className="btn btn-ghost" style={{ padding:"4px 12px",fontSize:11, color: "var(--accent-red)" }} onClick={() => deleteJob(h.job_id)}>🗑 Delete</button>
                            </>
                          )}
                          {canResumeFromStopped(h) && (
                            <button
                              className="btn btn-ghost"
                              style={{ padding:"4px 12px",fontSize:11 }}
                              onClick={() => resumeFromRow(h, false)}
                              disabled={workingJobId === h.job_id}
                            >
                              Resume
                            </button>
                          )}
                          {canResumeFromStopped(h) && (
                            <button
                              className="btn btn-ghost"
                              style={{ padding:"4px 12px",fontSize:11 }}
                              onClick={() => resumeFromRow(h, true)}
                              disabled={workingJobId === h.job_id}
                            >
                              Restart all
                            </button>
                          )}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>

      {viewingJob && (
        <div style={{ position: "fixed", top: 0, left: 0, right: 0, bottom: 0, background: "var(--bg-void)", zIndex: 1000, display: "flex", flexDirection: "column" }} className="anim-fade-in">
          <div style={{ padding: "20px 40px", borderBottom: "1px solid var(--border)", display: "flex", justifyContent: "space-between", alignItems: "center", background: "var(--bg-deep)" }}>
            <div>
              <h2 style={{ fontSize: 20, fontWeight: 800, letterSpacing: "-0.02em" }}>Data Explorer</h2>
              <p style={{ fontSize: 12, color: "var(--text-secondary)", marginTop: 4 }}>
                {viewingJob.profession} in {viewingJob.location} — {viewingLeads.length} leads found
              </p>
            </div>
            <div style={{ display: "flex", gap: 12 }}>
              <button className="btn btn-primary" onClick={() => download(viewingJob.job_id, `${viewingJob.profession}_auto.csv`)}>Download CSV</button>
              <button className="btn btn-ghost" onClick={() => setViewingJob(null)}>Close</button>
            </div>
          </div>
          <div style={{ flex: 1, overflow: "auto", padding: 20 }}>
            {viewingLoading ? (
              <div style={{ textAlign: "center", padding: 60, color: "var(--text-muted)" }}>Loading entire dataset...</div>
            ) : viewingLeads.length === 0 ? (
              <div style={{ textAlign: "center", padding: 60, color: "var(--text-muted)" }}>No data available.</div>
            ) : (
              <table className="data-table">
                <thead><tr><th>Business</th><th>Phone</th><th>Owner</th><th>Email</th><th>Website</th><th>Socials</th></tr></thead>
                <tbody>
                  {viewingLeads.map((lead, i) => (
                    <tr key={i}>
                      <td>
                        <a href={lead["Maps URL"]} target="_blank" rel="noreferrer" style={{ color: "var(--text-primary)", textDecoration: "none", fontWeight: 600 }}>
                          {lead.Name || "—"} ↗
                        </a>
                        <div style={{ fontSize: 10, color: "var(--text-muted)", marginTop: 4 }}>{lead.Category}</div>
                        <div style={{ fontSize: 10, color: "var(--text-muted)", marginTop: 2, maxWidth: 250, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{lead.Address}</div>
                      </td>
                      <td>{lead.Phone || "—"}</td>
                      <td style={{ color: lead.Owner_Name ? "var(--accent-violet)" : "var(--text-muted)", fontWeight: lead.Owner_Name ? 600 : 400 }}>
                        {lead.Owner_Name || "—"}
                      </td>
                      <td style={{ maxWidth: 180, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                        {lead.Email || lead.Owner_Email_Guesses?.split(" | ")[0] || "—"}
                      </td>
                      <td>
                        {lead.Website ? (
                          <a href={lead.Website} target="_blank" rel="noreferrer" className="badge badge-green" style={{ textDecoration: "none", display: "inline-block" }}>Website ↗</a>
                        ) : <span className="badge badge-red">No Website</span>}
                      </td>
                      <td>
                        <div style={{ display: "flex", gap: 4 }}>
                          {lead.LinkedIn && <a href={lead.LinkedIn} target="_blank" rel="noreferrer" className="badge badge-cyan" style={{ textDecoration: "none" }}>IN ↗</a>}
                          {lead.Facebook && <a href={lead.Facebook} target="_blank" rel="noreferrer" className="badge badge-cyan" style={{ textDecoration: "none", background: "rgba(59, 130, 246, 0.1)", color: "#3b82f6" }}>FB ↗</a>}
                          {lead.Instagram && <a href={lead.Instagram} target="_blank" rel="noreferrer" className="badge badge-cyan" style={{ textDecoration: "none", background: "rgba(236, 72, 153, 0.1)", color: "#ec4899" }}>IG ↗</a>}
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
