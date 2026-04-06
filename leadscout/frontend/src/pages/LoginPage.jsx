import React from "react";
import { useState } from "react"
import { Link, useNavigate } from "react-router-dom"
import SparklesBg from "../components/SparklesBg"
import { redirectToWaitlist } from "../lib/waitlist"



export default function LoginPage({ onLogin }) {
  const [mode, setMode]     = useState("login")
  const [form, setForm]     = useState({ name: "", email: "", password: "" })
  const [error, setError]   = useState("")
  const [loading, setLoading] = useState(false)
  const navigate = useNavigate()

  const submit = async () => {
    setError(""); setLoading(true)
    try {
      const res  = await fetch(`/api${mode === "login" ? "/auth/login" : "/auth/register"}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(form),
      })
      const data = await res.json()
      if (!res.ok) {
        if (res.status === 403 && data.detail?.includes("Waitlist active")) {
          redirectToWaitlist({ reason: "access_restricted", source: "login" })
          return
        }
        throw new Error(data.detail || "Failed")
      }
      
      onLogin({ token: data.token, user: data.user })
      navigate("/dashboard")
    } catch (e) { 
      setError(e.message) 
    }
    setLoading(false)
  }

  return (
    <div className="page" style={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "100vh" }}>
      <SparklesBg />
      <div style={{ width: "100%", maxWidth: 420, padding: "0 24px", zIndex: 1 }} className="anim-fade-up">
        <Link to="/" style={{ display: "block", textAlign: "center", marginBottom: 40, textDecoration: "none" }}>
          <span style={{ fontSize: 28, fontWeight: 800, letterSpacing: "-0.02em", color: "var(--text-primary)" }}>
            Lead<span style={{ color: "var(--accent-cyan)" }}>Scout</span>
          </span>
        </Link>

        <div className="card card-glow">
          <h2 style={{ fontSize: 20, fontWeight: 800, letterSpacing: "-0.02em", marginBottom: 4 }}>
            {mode === "login" ? "Welcome back" : "Create account"}
          </h2>
          <p style={{ fontSize: 13, color: "var(--text-secondary)", marginBottom: 28 }}>
            {mode === "login" ? "Sign in to your LeadScout account" : "Start scraping leads for free"}
          </p>

          {mode === "register" && (
            <div style={{ marginBottom: 16 }}>
              <label>Full name</label>
              <input placeholder="Mohammed Ainan" value={form.name} onChange={e => setForm({ ...form, name: e.target.value })} />
            </div>
          )}
          <div style={{ marginBottom: 16 }}>
            <label>Email</label>
            <input type="email" placeholder="you@example.com" value={form.email} onChange={e => setForm({ ...form, email: e.target.value })} />
          </div>
          <div style={{ marginBottom: 24 }}>
            <label>Password</label>
            <input type="password" placeholder="••••••••" value={form.password} onChange={e => setForm({ ...form, password: e.target.value })} onKeyDown={e => e.key === "Enter" && submit()} />
          </div>

          {error && (
            <div style={{ background: "rgba(252,129,129,0.08)", border: "1px solid rgba(252,129,129,0.2)", borderRadius: "var(--radius-md)", padding: "10px 14px", fontSize: 13, color: "var(--accent-red)", marginBottom: 16 }}>{error}</div>
          )}

          <button className="btn btn-primary" style={{ width: "100%", justifyContent: "center", padding: 13, fontSize: 14 }} onClick={submit} disabled={loading}>
            {loading ? "..." : mode === "login" ? "Sign in →" : "Create account →"}
          </button>

          <div className="divider" />
          <p style={{ textAlign: "center", fontSize: 13, color: "var(--text-secondary)" }}>
            {mode === "login" ? "Don't have an account? " : "Already have an account? "}
            <button style={{ background: "none", border: "none", color: "var(--accent-cyan)", cursor: "pointer", fontFamily: "var(--font-display)", fontSize: 13 }}
              onClick={() => { setMode(mode === "login" ? "register" : "login"); setError("") }}>
              {mode === "login" ? "Sign up" : "Sign in"}
            </button>
          </p>
        </div>
      </div>
    </div>
  )
}
