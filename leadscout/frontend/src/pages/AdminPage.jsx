import React from "react";
import { useEffect, useState } from "react"
import { Link, useNavigate } from "react-router-dom"
import Nav from "../components/Nav"
import SparklesBg from "../components/SparklesBg"
import { authFetch } from "../lib/auth"

const PLAN_OPTIONS = ["starter", "pro", "growth", "team"]
const ROLE_OPTIONS = ["user", "admin"]

export default function AdminPage({ user, onLogout }) {
  const navigate = useNavigate()
  const [users, setUsers] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState("")
  const [savingId, setSavingId] = useState("")
  const [banner, setBanner] = useState("")

  const normalizedRole = String(user?.role || "").trim().toLowerCase()

  useEffect(() => {
    if (!user) return
    console.debug("[AdminPage] role check", { rawRole: user?.role, normalizedRole })
    if (normalizedRole !== "admin") {
      setLoading(false)
      return
    }

    let active = true
    authFetch("/admin/users", {}, () => navigate("/login"))
      .then(async (res) => {
        const data = await res.json().catch(() => ({}))
        if (!active) return
        if (!res.ok) {
          throw new Error(data?.detail || "Unable to load admin users")
        }
        setUsers((data.users || []).map((row) => ({
          ...row,
          draftPlan: row.plan || "starter",
          draftRole: row.role || "user",
        })))
        setError("")
      })
      .catch((err) => {
        if (active) setError(String(err?.message || "Unable to load admin users"))
      })
      .finally(() => {
        if (active) setLoading(false)
      })

    return () => {
      active = false
    }
  }, [navigate, normalizedRole, user])

  const isAdmin = normalizedRole === "admin"

  const updateDraft = (id, key, value) => {
    setUsers((prev) => prev.map((row) => (row.id === id ? { ...row, [key]: value } : row)))
  }

  const saveRow = async (row) => {
    setSavingId(row.id)
    setError("")
    setBanner("")
    try {
      const res = await authFetch("/admin/update-user", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          user_id: row.id,
          plan: row.draftPlan,
          role: row.draftRole,
        }),
      }, () => navigate("/login"))
      const data = await res.json().catch(() => ({}))
      if (!res.ok) {
        throw new Error(data?.message || data?.detail || "Unable to update user")
      }
      const nextUser = data?.user || data?.data?.user || {}
      setUsers((prev) =>
        prev.map((item) =>
          item.id === row.id
            ? {
                ...item,
                plan: nextUser.plan || row.draftPlan,
                role: nextUser.role || row.draftRole,
                draftPlan: nextUser.plan || row.draftPlan,
                draftRole: nextUser.role || row.draftRole,
              }
            : item
        )
      )
      setBanner(`Updated ${row.email || row.name || "user"} successfully.`)
    } catch (err) {
      setError(String(err?.message || "Unable to update user"))
    } finally {
      setSavingId("")
    }
  }

  return (
    <div className="page">
      <SparklesBg />
      <Nav user={user} onLogout={onLogout} />
      <div style={{ paddingTop: 64 }}>
        <div className="admin-page-shell" style={{ maxWidth: 1080, margin: "0 auto", padding: "40px 24px 80px" }}>
          {!isAdmin ? (
            <div className="card" style={{ textAlign: "center" }}>
              <span className="badge badge-red" style={{ marginBottom: 12, display: "inline-block" }}>Access Denied</span>
              <h1 style={{ fontSize: 28, fontWeight: 800, letterSpacing: "-0.02em", marginBottom: 12 }}>Admin role required</h1>
              <p style={{ color: "var(--text-secondary)", lineHeight: 1.7, marginBottom: 18 }}>
                This page is now protected by your backend user role, not frontend email allowlists.
              </p>
              <p style={{ color: "var(--text-muted)", fontSize: 12, marginBottom: 18 }}>
                Current role: {user?.role || "missing"}
              </p>
              <Link to="/dashboard" className="btn btn-ghost">Back to dashboard</Link>
            </div>
          ) : (
            <div className="card anim-fade-up">
              <span className="badge badge-cyan" style={{ marginBottom: 12, display: "inline-block" }}>Admin</span>
              <h1 style={{ fontSize: 28, fontWeight: 800, letterSpacing: "-0.02em", marginBottom: 8 }}>LeadScout admin</h1>
              <p style={{ color: "var(--text-secondary)", lineHeight: 1.7, marginBottom: 24 }}>
                Backend-backed admin access is enabled for your account role. This panel reflects the current users table from the API.
              </p>
              <p style={{ color: "var(--text-muted)", fontSize: 12, marginBottom: 24 }}>
                Current role: {user?.role || "missing"}
              </p>
              <p style={{ color: "var(--text-muted)", fontSize: 12, marginBottom: 24 }}>
                Team plan is unlimited for internal staff. Admin role remains fully unlimited regardless of plan.
              </p>

              {loading ? (
                <div style={{ color: "var(--text-muted)" }}>Loading users…</div>
              ) : error ? (
                <div style={{ color: "var(--accent-red)" }}>{error}</div>
              ) : (
                <>
                  {banner && <div style={{ color: "var(--accent-cyan)", marginBottom: 18 }}>{banner}</div>}
                  <div className="mobile-only admin-mobile-list">
                    {users.map((row) => (
                      <div key={`mobile-${row.id}`} className="admin-mobile-card">
                        <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center" }}>
                          <div style={{ fontWeight: 600, color: "var(--text-primary)" }}>{row.name || "—"}</div>
                          <span className={`badge ${row.role === "admin" ? "badge-cyan" : "badge-gold"}`}>{row.role || "user"}</span>
                        </div>
                        <div className="admin-mobile-meta">
                          <div>Email: {row.email || "—"}</div>
                          <div>Plan: {row.plan || "starter"}</div>
                          <div>Role: {row.role || "user"}</div>
                          <div>Jobs: {row.job_count || 0}</div>
                          <div>Total Leads: {row.total_leads || 0}</div>
                        </div>
                        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, marginTop: 12 }}>
                          <select value={row.draftPlan || "starter"} onChange={(e) => updateDraft(row.id, "draftPlan", e.target.value)}>
                            {PLAN_OPTIONS.map((option) => <option key={option} value={option}>{option}</option>)}
                          </select>
                          <select value={row.draftRole || "user"} onChange={(e) => updateDraft(row.id, "draftRole", e.target.value)}>
                            {ROLE_OPTIONS.map((option) => <option key={option} value={option}>{option}</option>)}
                          </select>
                        </div>
                        <button
                          className="btn btn-ghost"
                          style={{ marginTop: 12, width: "100%", justifyContent: "center" }}
                          onClick={() => saveRow(row)}
                          disabled={savingId === row.id}
                        >
                          {savingId === row.id ? "Saving..." : "Save Update"}
                        </button>
                      </div>
                    ))}
                  </div>
                  <div className="table-wrap desktop-only">
                    <table className="data-table">
                      <thead>
                        <tr>
                          <th>Name</th>
                          <th>Email</th>
                          <th>Plan</th>
                          <th>Role</th>
                          <th>Jobs</th>
                          <th>Total Leads</th>
                          <th>Update</th>
                        </tr>
                      </thead>
                      <tbody>
                        {users.map((row) => (
                          <tr key={row.id}>
                            <td>{row.name || "—"}</td>
                            <td>{row.email || "—"}</td>
                            <td>
                              <select value={row.draftPlan || "starter"} onChange={(e) => updateDraft(row.id, "draftPlan", e.target.value)}>
                                {PLAN_OPTIONS.map((option) => <option key={option} value={option}>{option}</option>)}
                              </select>
                            </td>
                            <td>
                              <select value={row.draftRole || "user"} onChange={(e) => updateDraft(row.id, "draftRole", e.target.value)}>
                                {ROLE_OPTIONS.map((option) => <option key={option} value={option}>{option}</option>)}
                              </select>
                            </td>
                            <td>{row.job_count || 0}</td>
                            <td>{row.total_leads || 0}</td>
                            <td>
                              <button
                                className="btn btn-ghost"
                                style={{ padding: "4px 12px", fontSize: 11 }}
                                onClick={() => saveRow(row)}
                                disabled={savingId === row.id}
                              >
                                {savingId === row.id ? "Saving..." : "Save"}
                              </button>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
