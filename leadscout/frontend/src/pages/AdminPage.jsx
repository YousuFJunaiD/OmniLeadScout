import React from "react";
import { useEffect, useState } from "react"
import { Link, useNavigate } from "react-router-dom"
import Nav from "../components/Nav"
import SparklesBg from "../components/SparklesBg"
import { authFetch } from "../lib/auth"

export default function AdminPage({ user, onLogout }) {
  const navigate = useNavigate()
  const [users, setUsers] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState("")

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
        setUsers(data.users || [])
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

  return (
    <div className="page">
      <SparklesBg />
      <Nav user={user} onLogout={onLogout} />
      <div style={{ paddingTop: 64 }}>
        <div style={{ maxWidth: 1080, margin: "0 auto", padding: "40px 24px 80px" }}>
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

              {loading ? (
                <div style={{ color: "var(--text-muted)" }}>Loading users…</div>
              ) : error ? (
                <div style={{ color: "var(--accent-red)" }}>{error}</div>
              ) : (
                <div className="table-wrap">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Name</th>
                        <th>Email</th>
                        <th>Plan</th>
                        <th>Role</th>
                        <th>Jobs</th>
                        <th>Total Leads</th>
                      </tr>
                    </thead>
                    <tbody>
                      {users.map((row) => (
                        <tr key={row.id}>
                          <td>{row.name || "—"}</td>
                          <td>{row.email || "—"}</td>
                          <td>{row.plan || "starter"}</td>
                          <td><span className={`badge ${row.role === "admin" ? "badge-cyan" : "badge-gold"}`}>{row.role || "user"}</span></td>
                          <td>{row.job_count || 0}</td>
                          <td>{row.total_leads || 0}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
