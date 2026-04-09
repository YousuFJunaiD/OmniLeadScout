import React from "react";
import { useState } from "react"
import { Link, useNavigate } from "react-router-dom"
import SparklesBg from "../components/SparklesBg"
import { apiUrl, getApiHeaders } from "../lib/api"

const toReadableError = (value, fallback = "Something went wrong") => {
  if (!value) return fallback
  if (typeof value === "string") return value
  if (typeof value === "object") {
    return value.error || value.detail || value.message || fallback
  }
  return fallback
}

export default function LoginPage({ onLogin }) {
  const [mode, setMode]     = useState("login")
  const [form, setForm]     = useState({ name: "", email: "", password: "" })
  const [error, setError]   = useState("")
  const [loading, setLoading] = useState(false)
  const [showPassword, setShowPassword] = useState(false)
  const navigate = useNavigate()

  const submit = async () => {
    setError(""); setLoading(true)
    try {
      const res  = await fetch(apiUrl(mode === "login" ? "/auth/login" : "/auth/register"), {
        method: "POST",
        headers: getApiHeaders(),
        body: JSON.stringify(form),
      })
      const data = await res.json()
      if (!res.ok) {
        throw new Error(toReadableError(data?.detail || data, "Failed"))
      }
      
      onLogin({ token: data.token, user: data.user })
      navigate("/dashboard")
    } catch (e) { 
      setError(toReadableError(e?.message, "Failed")) 
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
              <input placeholder="Your full name" value={form.name} onChange={e => setForm({ ...form, name: e.target.value })} />
            </div>
          )}
          <div style={{ marginBottom: 16 }}>
            <label>Email</label>
            <input type="email" placeholder="you@example.com" value={form.email} onChange={e => setForm({ ...form, email: e.target.value })} />
          </div>
          <div style={{ marginBottom: 24 }}>
            <label>Password</label>
            <div style={{ position: "relative" }}>
              <input
                type={showPassword ? "text" : "password"}
                placeholder="••••••••"
                value={form.password}
                onChange={e => setForm({ ...form, password: e.target.value })}
                onKeyDown={e => e.key === "Enter" && submit()}
                style={{ paddingRight: 88 }}
              />
              <button
                type="button"
                onClick={() => setShowPassword((value) => !value)}
                style={{
                  position: "absolute",
                  right: 12,
                  top: "50%",
                  transform: "translateY(-50%)",
                  background: "none",
                  border: "none",
                  color: "var(--text-secondary)",
                  cursor: "pointer",
                  fontSize: 12,
                  letterSpacing: "0.08em",
                  textTransform: "uppercase",
                  fontFamily: "var(--font-display)",
                }}
              >
                {showPassword ? "Hide" : "Show"}
              </button>
            </div>
            {mode === "login" && (
              <div style={{ display: "flex", justifyContent: "flex-end", marginTop: 10 }}>
                <Link
                  to={form.email.trim() ? `/reset-password?email=${encodeURIComponent(form.email.trim())}` : "/reset-password"}
                  style={{ color: "var(--accent-cyan)", textDecoration: "none", fontSize: 12, letterSpacing: "0.03em" }}
                >
                  Forgot password?
                </Link>
              </div>
            )}
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
