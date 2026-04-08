import React from "react";
import { useEffect, useMemo, useState } from "react"
import { Link } from "react-router-dom"
import Nav from "../components/Nav"
import SparklesBg from "../components/SparklesBg"
import { supabase } from "../lib/supabase"

const ADMIN_EMAILS = (import.meta.env.VITE_ADMIN_EMAILS || "")
  .split(",")
  .map((email) => email.trim().toLowerCase())
  .filter(Boolean)

export default function AdminPage({ user, onLogout }) {
  const [currentEmail, setCurrentEmail] = useState("")

  useEffect(() => {
    if (typeof window === "undefined") return

    let mounted = true
    const fallbackEmail = (window.localStorage.getItem("user_email") || user?.email || "").trim().toLowerCase()

    if (!supabase) {
      setCurrentEmail(fallbackEmail)
      return
    }

    supabase.auth.getSession().then(({ data }) => {
      if (!mounted) return
      const sessionEmail = data.session?.user?.email?.trim().toLowerCase() || fallbackEmail
      setCurrentEmail(sessionEmail)
    })

    const { data: listener } = supabase.auth.onAuthStateChange((_event, session) => {
      const sessionEmail = session?.user?.email?.trim().toLowerCase() || fallbackEmail
      setCurrentEmail(sessionEmail)
    })

    return () => {
      mounted = false
      listener.subscription.unsubscribe()
    }
  }, [user?.email])

  const isAllowed = useMemo(() => {
    return Boolean(currentEmail) && ADMIN_EMAILS.includes(currentEmail)
  }, [currentEmail])

  if (!currentEmail) {
    return (
      <div className="page">
        <SparklesBg />
        <Nav user={user} onLogout={onLogout} />
        <div style={{ paddingTop: 64, minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center", padding: "96px 24px 40px" }}>
          <div className="card" style={{ maxWidth: 520, width: "100%", textAlign: "center" }}>
            <span className="badge badge-red" style={{ marginBottom: 12, display: "inline-block" }}>Admin Only</span>
            <h1 style={{ fontSize: 28, fontWeight: 800, letterSpacing: "-0.02em", marginBottom: 12 }}>Admin email missing</h1>
            <p style={{ color: "var(--text-secondary)", lineHeight: 1.7 }}>
              Sign in first, or set <code>localStorage.getItem("user_email")</code> to an allowed admin email.
            </p>
          </div>
        </div>
      </div>
    )
  }

  if (!isAllowed) {
    return (
      <div className="page">
        <SparklesBg />
        <Nav user={user} onLogout={onLogout} />
        <div style={{ paddingTop: 64, minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center", padding: "96px 24px 40px" }}>
          <div className="card" style={{ maxWidth: 560, width: "100%", textAlign: "center" }}>
            <span className="badge badge-red" style={{ marginBottom: 12, display: "inline-block" }}>Access Denied</span>
            <h1 style={{ fontSize: 28, fontWeight: 800, letterSpacing: "-0.02em", marginBottom: 12 }}>You do not have admin access</h1>
            <p style={{ color: "var(--text-secondary)", lineHeight: 1.7, marginBottom: 18 }}>
              Signed in as <strong style={{ color: "var(--text-primary)" }}>{currentEmail}</strong>. Ask the founder to add this email to <code>VITE_ADMIN_EMAILS</code>.
            </p>
            <Link to="/dashboard" className="btn btn-ghost">Back to dashboard</Link>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="page">
      <SparklesBg />
      <Nav user={user} onLogout={onLogout} />
      <div style={{ paddingTop: 64 }}>
        <div style={{ maxWidth: 920, margin: "0 auto", padding: "40px 24px 80px" }}>
          <div className="card anim-fade-up">
            <span className="badge badge-cyan" style={{ marginBottom: 12, display: "inline-block" }}>Admin</span>
            <h1 style={{ fontSize: 28, fontWeight: 800, letterSpacing: "-0.02em", marginBottom: 8 }}>LeadScout admin</h1>
            <p style={{ color: "var(--text-secondary)", lineHeight: 1.7, marginBottom: 24 }}>
              Admin access is still restricted by <code>VITE_ADMIN_EMAILS</code>, and you can manage users from the backend or Supabase directly while broader admin tools are rebuilt.
            </p>

            <div className="admin-kpi-grid" style={{ marginBottom: 24 }}>
              <div className="card" style={{ padding: "16px 20px" }}>
                <div style={{ fontSize: 10, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: 6 }}>Signed In As</div>
                <div style={{ fontSize: 16, fontWeight: 700, color: "var(--text-primary)" }}>{currentEmail}</div>
              </div>
              <div className="card" style={{ padding: "16px 20px" }}>
                <div style={{ fontSize: 10, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: 6 }}>App Access</div>
                <div style={{ fontSize: 16, fontWeight: 700, color: "var(--text-primary)" }}>Open signup and login</div>
              </div>
            </div>

            <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
              <Link to="/dashboard" className="btn btn-primary">Open dashboard</Link>
              <Link to="/pricing" className="btn btn-ghost">Open pricing</Link>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
