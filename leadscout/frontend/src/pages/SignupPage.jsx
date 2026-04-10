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

export default function SignupPage({ onLogin }) {
  const [form, setForm]     = useState({ name: "", email: "", password: "" })
  const [error, setError]   = useState("")
  const [loading, setLoading] = useState(false)
  const [showPassword, setShowPassword] = useState(false)
  const nav = useNavigate()

  const set = (k) => (e) => setForm(f => ({ ...f, [k]: e.target.value }))

  const submit = async () => {
    if (!form.name.trim() || !form.email.trim() || !form.password.trim()) {
      setError("All fields are required."); return
    }
    setError(""); setLoading(true)
    try {
      const res  = await fetch(apiUrl("/auth/register"), {
        method: "POST",
        headers: getApiHeaders(),
        body: JSON.stringify(form),
      })
      const data = await res.json()
      if (!res.ok) throw new Error(toReadableError(data?.detail || data, "Registration failed"))
      onLogin(data)
      nav("/dashboard")
    } catch (e) { setError(toReadableError(e?.message, "Registration failed")) }
    setLoading(false)
  }

  const field = {
    background: "#000",
    border: "1px solid rgba(255,255,255,0.18)",
    borderRadius: 0,
    color: "#fff",
    padding: "13px 16px",
    fontSize: 14,
    width: "100%",
    outline: "none",
    fontFamily: "var(--font-body)",
    letterSpacing: "0.01em",
    transition: "border-color 0.15s",
  }

  return (
    <div className="page auth-shell" style={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "100vh", background: "#000" }}>
      <SparklesBg />

      <div style={{ width: "100%", maxWidth: 420, padding: "0 24px", zIndex: 1 }} className="anim-fade-up">

        {/* Logo */}
        <Link to="/" style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 8, marginBottom: 44, textDecoration: "none" }}>
          <img src="/omnimate-logo.png" alt="OMNIMATE" style={{ height: 56, width: "auto", opacity: 0.9 }} />
          <span style={{ fontSize: 10, letterSpacing: "0.24em", textTransform: "uppercase", color: "rgba(255,255,255,0.3)", fontFamily: "var(--font-mono)" }}>
            LeadScout
          </span>
        </Link>

        {/* Card */}
        <div className="auth-card-shell" style={{ border: "1px solid rgba(255,255,255,0.14)", background: "#0A0A0A", padding: "36px 32px" }}>

          <div style={{ marginBottom: 28 }}>
            <h2 style={{ fontSize: 20, fontWeight: 800, letterSpacing: "0.04em", textTransform: "uppercase", color: "#fff", marginBottom: 6 }}>
              Create account
            </h2>
            <p style={{ fontSize: 12, color: "rgba(255,255,255,0.38)", letterSpacing: "0.04em" }}>
              Start collecting leads for free — no card required
            </p>
          </div>

          <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
            <div>
              <label>Full name</label>
              <input
                style={field}
                placeholder="Your name"
                value={form.name}
                onChange={set("name")}
                onFocus={e => { e.target.style.borderColor = "rgba(255,255,255,0.55)" }}
                onBlur={e  => { e.target.style.borderColor = "rgba(255,255,255,0.18)" }}
              />
            </div>
            <div>
              <label>Email</label>
              <input
                style={field}
                type="email"
                placeholder="you@company.com"
                value={form.email}
                onChange={set("email")}
                onFocus={e => { e.target.style.borderColor = "rgba(255,255,255,0.55)" }}
                onBlur={e  => { e.target.style.borderColor = "rgba(255,255,255,0.18)" }}
              />
            </div>
            <div>
              <label>Password</label>
              <div style={{ position: "relative" }}>
                <input
                  style={{ ...field, paddingRight: 88 }}
                  type={showPassword ? "text" : "password"}
                  placeholder="Min. 8 characters"
                  value={form.password}
                  onChange={set("password")}
                  onKeyDown={e => e.key === "Enter" && submit()}
                  onFocus={e => { e.target.style.borderColor = "rgba(255,255,255,0.55)" }}
                  onBlur={e  => { e.target.style.borderColor = "rgba(255,255,255,0.18)" }}
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
                    color: "rgba(255,255,255,0.62)",
                    cursor: "pointer",
                    fontSize: 11,
                    letterSpacing: "0.08em",
                    textTransform: "uppercase",
                    fontFamily: "var(--font-display)",
                  }}
                >
                  {showPassword ? "Hide" : "Show"}
                </button>
              </div>
            </div>
          </div>

          {error && (
            <div style={{ marginTop: 16, border: "1px solid rgba(255,80,80,0.3)", padding: "10px 14px", fontSize: 12, color: "rgba(255,100,100,0.9)", letterSpacing: "0.03em" }}>
              {error}
            </div>
          )}

          <button
            className="btn btn-primary"
            style={{ width: "100%", justifyContent: "center", padding: "13px 0", fontSize: 12, marginTop: 24, letterSpacing: "0.12em" }}
            onClick={submit}
            disabled={loading}
          >
            {loading ? "Creating account…" : "Create Account →"}
          </button>

          <div style={{ height: 1, background: "rgba(255,255,255,0.08)", margin: "24px 0" }} />

          <p style={{ textAlign: "center", fontSize: 12, color: "rgba(255,255,255,0.38)", letterSpacing: "0.04em" }}>
            Already have an account?{" "}
            <Link to="/login" style={{ color: "#fff", textDecoration: "none", borderBottom: "1px solid rgba(255,255,255,0.3)", paddingBottom: 1 }}>
              Sign in
            </Link>
          </p>
        </div>

        <p style={{ textAlign: "center", marginTop: 20, fontSize: 11, color: "rgba(255,255,255,0.2)", letterSpacing: "0.06em" }}>
          By signing up you agree to our terms of service
        </p>
      </div>
    </div>
  )
}
