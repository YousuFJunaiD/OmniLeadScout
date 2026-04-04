import { useEffect, useState } from "react"
import { useNavigate } from "react-router-dom"
import Nav from "../components/Nav"
import SparklesBg from "../components/SparklesBg"
import { authFetch } from "../lib/auth"

const API = import.meta.env.VITE_API_URL || "http://localhost:8000"

export default function AdminPage({ user, onLogout }) {
  const navigate = useNavigate()
  const [users, setUsers] = useState([])
  const [jobs, setJobs]   = useState([])
  const [stats, setStats] = useState({})
  const [tab, setTab]     = useState("overview")
  const [loading, setLoading] = useState(true)
  const [savingUserId, setSavingUserId] = useState("")
  const authRequest = (url, options = {}) => authFetch(url, options, () => navigate("/login"))

  const loadAdmin = () => {
    setLoading(true)
    Promise.all([
      authRequest(`${API}/admin/users`).then(r => r.json()),
      authRequest(`${API}/admin/jobs`).then(r => r.json()),
      authRequest(`${API}/admin/stats`).then(r => r.json()),
    ]).then(([u, j, s]) => { setUsers(u.users||[]); setJobs(j.jobs||[]); setStats(s); setLoading(false) })
    .catch(() => setLoading(false))
  }

  useEffect(() => {
    loadAdmin()
  }, [user.id])

  const updateUser = async (targetUser) => {
    setSavingUserId(targetUser.id)
    try {
      const res = await authRequest(`${API}/admin/update-user`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          user_id: targetUser.id,
          plan: targetUser.plan || null,
          role: targetUser.role || null,
        }),
      })
      if (!res.ok) throw new Error("update failed")
      loadAdmin()
    } catch {
      alert("Failed to update user")
    } finally {
      setSavingUserId("")
    }
  }

  const dlAny = async (jobId, name) => {
      const res  = await authRequest(`${API}/scrape/download/${jobId}`)
    const blob = await res.blob()
    const url  = URL.createObjectURL(blob)
    const a    = document.createElement("a")
    a.href = url; a.download = name; a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <div className="page">
      <SparklesBg />
      <Nav user={user} onLogout={onLogout} />
      <div style={{ paddingTop: 64 }}>
        <div style={{ maxWidth: 1200, margin: "0 auto", padding: "40px 24px" }} className="admin-shell">

          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", marginBottom: 28 }} className="anim-fade-up">
            <div>
              <span className="badge badge-red" style={{ marginBottom: 8, display: "inline-block" }}>Admin Only</span>
              <h1 style={{ fontSize: 28, fontWeight: 800, letterSpacing: "-0.02em" }}>Control Panel</h1>
            </div>
            <div className="stat-pill"><span className="dot" /> System operational</div>
          </div>

          <div className="admin-kpi-grid stagger" style={{ marginBottom: 24 }}>
            {[["Total Users",stats.total_users||0,"var(--accent-cyan)"],["Total Jobs",stats.total_jobs||0,"var(--accent-violet)"],["Total Leads",(stats.total_leads||0).toLocaleString(),"var(--accent-gold)"],["Active Now",stats.active_jobs||0,"var(--accent-green)"],["CSV Files",stats.total_files||0,"var(--text-secondary)"]].map(([l,v,c],i) => (
              <div key={i} className="card" style={{ padding: "16px 20px" }}>
                <div style={{ fontSize: 10, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: 6 }}>{l}</div>
                <div style={{ fontSize: 26, fontWeight: 800, color: c, letterSpacing: "-0.02em", fontFamily: "var(--font-mono)" }}>{v}</div>
              </div>
            ))}
          </div>

          <div style={{ display: "flex", gap: 4, marginBottom: 20, borderBottom: "1px solid var(--border)" }}>
            {["overview","users","jobs"].map(t => (
              <button key={t} onClick={() => setTab(t)} style={{ padding: "10px 20px", fontSize: 13, fontWeight: 600, background: "transparent", border: "none", borderBottom: `2px solid ${tab===t?"var(--accent-cyan)":"transparent"}`, color: tab===t?"var(--accent-cyan)":"var(--text-secondary)", cursor: "pointer", fontFamily: "var(--font-display)", textTransform: "capitalize", marginBottom: -1 }}>
                {t}
              </button>
            ))}
          </div>

          {tab === "overview" && (
            <div className="card anim-fade-in">
              {loading && <p style={{ fontSize: 13, color: "var(--text-secondary)", marginBottom: 14 }}>Loading admin data...</p>}
              <p style={{ fontSize: 13, color: "var(--text-secondary)", lineHeight: 1.8, marginBottom: 20 }}>
                Use <strong style={{ color: "var(--text-primary)" }}>Users</strong> tab to see all accounts. Use <strong style={{ color: "var(--text-primary)" }}>Jobs</strong> tab to see every scrape and download any CSV.
              </p>
              <div className="divider" />
              <div className="admin-overview-grid" style={{ gap: 16 }}>
                {[["Most active profession",stats.top_profession],["Most scraped city",stats.top_location],["Avg leads per job",stats.avg_leads]].map(([l,v],i) => (
                  <div key={i}>
                    <div style={{ fontSize: 10, color: "var(--text-muted)", marginBottom: 4, textTransform: "uppercase", letterSpacing: "0.08em" }}>{l}</div>
                    <div style={{ fontSize: 15, fontWeight: 600 }}>{v||"—"}</div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {tab === "users" && (
            <div className="card anim-fade-in">
              <div className="table-wrap">
                <table className="data-table">
                  <thead><tr><th>User</th><th>Email</th><th>Plan</th><th>Role</th><th>Scrapes</th><th>Total Leads</th><th>Joined</th><th></th></tr></thead>
                  <tbody>
                    {users.length === 0 && !loading ? (
                      <tr><td colSpan={8} style={{ textAlign: "center", color: "var(--text-muted)" }}>No users found</td></tr>
                    ) : users.map((u,i) => (
                      <tr key={i}>
                        <td>{u.name}</td><td>{u.email}</td>
                        <td>
                          <select value={u.plan || ""} onChange={(e) => setUsers(prev => prev.map(row => row.id === u.id ? { ...row, plan: e.target.value } : row))}>
                            <option value="">None</option>
                            <option value="starter">starter</option>
                            <option value="pro">pro</option>
                            <option value="growth">growth</option>
                            <option value="enterprise">enterprise</option>
                          </select>
                        </td>
                        <td>
                          <select value={u.role || "user"} onChange={(e) => setUsers(prev => prev.map(row => row.id === u.id ? { ...row, role: e.target.value } : row))}>
                            <option value="user">user</option>
                            <option value="admin">admin</option>
                          </select>
                        </td>
                        <td>{u.job_count||0}</td>
                        <td style={{ color:"var(--accent-cyan)" }}>{(u.total_leads||0).toLocaleString()}</td>
                        <td style={{ fontSize:11 }}>{u.created_at?new Date(u.created_at).toLocaleDateString():"—"}</td>
                        <td>
                          <button className="btn btn-ghost" style={{ padding:"4px 12px",fontSize:11 }} onClick={() => updateUser(u)} disabled={savingUserId === u.id}>
                            {savingUserId === u.id ? "Saving..." : "Save"}
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {tab === "jobs" && (
            <div className="card anim-fade-in">
              <div className="table-wrap">
                <table className="data-table">
                  <thead><tr><th>Job ID</th><th>User</th><th>Profession</th><th>Location</th><th>Leads</th><th>Status</th><th>Date</th><th></th></tr></thead>
                  <tbody>
                    {jobs.length === 0 && !loading ? (
                      <tr><td colSpan={8} style={{ textAlign: "center", color: "var(--text-muted)" }}>No jobs found</td></tr>
                    ) : jobs.map((j,i) => (
                      <tr key={i}>
                        <td style={{ fontFamily:"var(--font-mono)",fontSize:11,color:"var(--text-muted)" }}>#{j.job_id?.slice(0,8)}</td>
                        <td>{j.user_name||j.user_id}</td><td>{j.profession}</td><td>{j.location}</td>
                        <td style={{ color:"var(--accent-cyan)",fontFamily:"var(--font-mono)" }}>{Number(j.effective_lead_count ?? j.lead_count ?? 0).toLocaleString()}</td>
                        <td><span className={`badge ${j.status==="completed"?"badge-green":j.status==="running"?"badge-cyan":"badge-red"}`}>{j.status}</span></td>
                        <td style={{ fontSize:11 }}>{j.created_at?new Date(j.created_at).toLocaleDateString():"—"}</td>
                        <td>{((j.status==="completed" || j.status==="stopped") && Number(j.effective_lead_count ?? j.lead_count ?? 0) > 0) && <button className="btn btn-ghost" style={{ padding:"4px 12px",fontSize:11 }} onClick={()=>dlAny(j.job_id,`admin_${j.profession}_${j.job_id?.slice(0,8)}.csv`)}>↓ CSV</button>}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
