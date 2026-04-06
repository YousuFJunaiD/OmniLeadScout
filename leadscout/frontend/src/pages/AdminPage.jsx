import React from "react";
import { useEffect, useMemo, useState } from "react"
import { Link } from "react-router-dom"
import Nav from "../components/Nav"
import SparklesBg from "../components/SparklesBg"
import { apiUrl, getApiHeaders } from "../lib/api"
import { supabase } from "../lib/supabase"

const ADMIN_EMAILS = (import.meta.env.VITE_ADMIN_EMAILS || "")
  .split(",")
  .map((email) => email.trim().toLowerCase())
  .filter(Boolean)

export default function AdminPage({ user, onLogout }) {
  const [currentEmail, setCurrentEmail] = useState("")
  const [waitlist, setWaitlist] = useState([])
  const [loading, setLoading] = useState(true)
  const [approvingEmail, setApprovingEmail] = useState("")
  const [feedback, setFeedback] = useState(null)

  useEffect(() => {
    if (typeof window === "undefined") return

    let mounted = true
    const fallbackEmail = (window.localStorage.getItem("user_email") || "").trim().toLowerCase()

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
  }, [])

  const isAllowed = useMemo(() => {
    return Boolean(currentEmail) && ADMIN_EMAILS.includes(currentEmail)
  }, [currentEmail])

  useEffect(() => {
    if (!isAllowed) {
      setLoading(false)
      return
    }

    let cancelled = false

    const loadWaitlist = async () => {
      setLoading(true)
      setFeedback(null)
      try {
        const res = await fetch(apiUrl("/waitlist/all"), {
          headers: getApiHeaders(),
        })
        const data = await res.json()
        if (!res.ok) throw new Error("Failed to load waitlist")
        if (!cancelled) {
          setWaitlist(Array.isArray(data) ? data : [])
        }
      } catch {
        if (!cancelled) {
          setFeedback({ tone: "error", text: "Failed to load waitlist users." })
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    loadWaitlist()
    return () => {
      cancelled = true
    }
  }, [isAllowed])

  const approve = async (email) => {
    setApprovingEmail(email)
    setFeedback(null)
    try {
      const res = await fetch(apiUrl("/waitlist/approve"), {
        method: "POST",
        headers: getApiHeaders(),
        body: JSON.stringify({ email }),
      })
      const data = await res.json().catch(() => null)
      if (!res.ok) throw new Error(data?.detail || "Approval failed")

      setWaitlist((prev) =>
        prev.map((entry) =>
          entry.email === email ? { ...entry, approved: 1 } : entry
        )
      )
      setFeedback({ tone: "success", text: `${email} approved successfully.` })
    } catch (error) {
      setFeedback({ tone: "error", text: error.message || "Approval failed." })
    } finally {
      setApprovingEmail("")
    }
  }

  const approvedCount = waitlist.filter((entry) => Number(entry.approved) === 1).length
  const pendingCount = waitlist.length - approvedCount

  const feedbackStyle = feedback?.tone === "success"
    ? { border: "1px solid rgba(97, 219, 165, 0.28)", background: "rgba(97, 219, 165, 0.08)", color: "rgba(220,255,235,0.92)" }
    : { border: "1px solid rgba(255,110,110,0.28)", background: "rgba(255,110,110,0.08)", color: "rgba(255,185,185,0.96)" }

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
              This page checks <code>localStorage.getItem("user_email")</code>. Set that value and make sure it exists in <code>VITE_ADMIN_EMAILS</code>.
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
            <Link to="/" className="btn btn-ghost">Back to home</Link>
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
        <div style={{ maxWidth: 1120, margin: "0 auto", padding: "40px 24px 80px" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-end", gap: 24, marginBottom: 28, flexWrap: "wrap" }} className="anim-fade-up">
            <div>
              <span className="badge badge-red" style={{ marginBottom: 10, display: "inline-block" }}>Admin Only</span>
              <h1 style={{ fontSize: 28, fontWeight: 800, letterSpacing: "-0.02em", marginBottom: 6 }}>Waitlist Approvals</h1>
              <p style={{ color: "var(--text-secondary)", fontSize: 14 }}>
                Review waitlist entries and approve teammates without leaving the dashboard.
              </p>
            </div>
            <div className="stat-pill"><span className="dot" /> {currentEmail}</div>
          </div>

          <div className="admin-kpi-grid stagger" style={{ marginBottom: 24 }}>
            {[["Total Waitlist", waitlist.length], ["Approved", approvedCount], ["Pending", pendingCount]].map(([label, value]) => (
              <div key={label} className="card" style={{ padding: "16px 20px" }}>
                <div style={{ fontSize: 10, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: 6 }}>{label}</div>
                <div style={{ fontSize: 26, fontWeight: 800, color: "var(--text-primary)", letterSpacing: "-0.02em", fontFamily: "var(--font-mono)" }}>{value}</div>
              </div>
            ))}
          </div>

          {feedback && (
            <div style={{ ...feedbackStyle, padding: "12px 16px", marginBottom: 18, fontSize: 13 }}>
              {feedback.text}
            </div>
          )}

          <div className="card anim-fade-in">
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 16, marginBottom: 18, flexWrap: "wrap" }}>
              <div>
                <p style={{ fontSize: 10, letterSpacing: "0.1em", textTransform: "uppercase", color: "var(--text-muted)", marginBottom: 6 }}>Waitlist Queue</p>
                <h2 style={{ fontSize: 20, fontWeight: 800, letterSpacing: "-0.02em" }}>Approval panel</h2>
              </div>
              {loading && <span style={{ color: "var(--text-secondary)", fontSize: 13 }}>Loading waitlist...</span>}
            </div>

            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Email</th>
                    <th>Joined</th>
                    <th>Status</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  {!loading && waitlist.length === 0 ? (
                    <tr>
                      <td colSpan={4} style={{ textAlign: "center", color: "var(--text-muted)" }}>No waitlist users found</td>
                    </tr>
                  ) : (
                    waitlist.map((entry) => {
                      const isApproved = Number(entry.approved) === 1
                      const isSaving = approvingEmail === entry.email
                      return (
                        <tr key={entry.email}>
                          <td>{entry.email}</td>
                          <td style={{ fontSize: 12 }}>{entry.created_at ? new Date(entry.created_at).toLocaleString() : "—"}</td>
                          <td>
                            <span className={`badge ${isApproved ? "badge-green" : "badge-red"}`}>
                              {isApproved ? "approved" : "pending"}
                            </span>
                          </td>
                          <td style={{ textAlign: "right" }}>
                            {!isApproved && (
                              <button
                                className="btn btn-primary"
                                style={{ padding: "6px 14px", fontSize: 11 }}
                                onClick={() => approve(entry.email)}
                                disabled={isSaving}
                              >
                                {isSaving ? "Approving..." : "Approve"}
                              </button>
                            )}
                          </td>
                        </tr>
                      )
                    })
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
