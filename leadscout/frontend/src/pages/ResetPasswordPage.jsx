import React from "react";
import { useMemo, useState } from "react"
import { Link, useNavigate, useSearchParams } from "react-router-dom"
import SparklesBg from "../components/SparklesBg"
import { apiUrl, getApiHeaders } from "../lib/api"

export default function ResetPasswordPage() {
  const [searchParams] = useSearchParams()
  const navigate = useNavigate()
  const token = searchParams.get("token") || ""
  const initialEmail = searchParams.get("email") || ""
  const [email, setEmail] = useState(initialEmail)
  const [password, setPassword] = useState("")
  const [confirmPassword, setConfirmPassword] = useState("")
  const [loading, setLoading] = useState(false)
  const [showPassword, setShowPassword] = useState(false)
  const [message, setMessage] = useState(null)

  const isResetMode = Boolean(token)
  const field = useMemo(() => ({
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
  }), [])

  const submitEmail = async () => {
    if (!email.trim()) {
      setMessage({ tone: "error", text: "Enter your account email first." })
      return
    }
    setLoading(true)
    setMessage(null)
    try {
      const res = await fetch(apiUrl("/auth/forgot-password"), {
        method: "POST",
        headers: getApiHeaders(),
        body: JSON.stringify({ email: email.trim() }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(data.detail || "Unable to send reset link")
      setMessage({ tone: "success", text: "If that email exists, we just sent a reset link." })
    } catch (error) {
      setMessage({ tone: "error", text: error.message || "Unable to send reset link." })
    } finally {
      setLoading(false)
    }
  }

  const submitPassword = async () => {
    if (!password.trim()) {
      setMessage({ tone: "error", text: "Enter a new password." })
      return
    }
    if (password !== confirmPassword) {
      setMessage({ tone: "error", text: "Passwords do not match." })
      return
    }
    setLoading(true)
    setMessage(null)
    try {
      const res = await fetch(apiUrl("/auth/reset-password"), {
        method: "POST",
        headers: getApiHeaders(),
        body: JSON.stringify({ token, password }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(data.detail || "Unable to reset password")
      setMessage({ tone: "success", text: "Password updated. Redirecting to login…" })
      window.setTimeout(() => navigate("/login"), 900)
    } catch (error) {
      setMessage({ tone: "error", text: error.message || "Unable to reset password." })
    } finally {
      setLoading(false)
    }
  }

  const alertStyle = message?.tone === "success"
    ? { border: "1px solid rgba(97, 219, 165, 0.28)", background: "rgba(97, 219, 165, 0.08)", color: "rgba(220,255,235,0.92)" }
    : { border: "1px solid rgba(255,110,110,0.28)", background: "rgba(255,110,110,0.08)", color: "rgba(255,185,185,0.96)" }

  return (
    <div className="page" style={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "100vh", background: "#000" }}>
      <SparklesBg />
      <div style={{ width: "100%", maxWidth: 420, padding: "0 24px", zIndex: 1 }} className="anim-fade-up">
        <Link to="/" style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 8, marginBottom: 44, textDecoration: "none" }}>
          <img src="/omnimate-logo.png" alt="OMNIMATE" style={{ height: 56, width: "auto", opacity: 0.9 }} />
          <span style={{ fontSize: 10, letterSpacing: "0.24em", textTransform: "uppercase", color: "rgba(255,255,255,0.3)", fontFamily: "var(--font-mono)" }}>
            LeadScout
          </span>
        </Link>

        <div style={{ border: "1px solid rgba(255,255,255,0.14)", background: "#0A0A0A", padding: "36px 32px" }}>
          <div style={{ marginBottom: 28 }}>
            <h2 style={{ fontSize: 20, fontWeight: 800, letterSpacing: "0.04em", textTransform: "uppercase", color: "#fff", marginBottom: 6 }}>
              {isResetMode ? "Choose a new password" : "Reset your password"}
            </h2>
            <p style={{ fontSize: 12, color: "rgba(255,255,255,0.38)", letterSpacing: "0.04em" }}>
              {isResetMode ? "Enter a new password to regain access to LeadScout." : "Enter your email and we&apos;ll send a secure reset link."}
            </p>
          </div>

          {!isResetMode ? (
            <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
              <div>
                <label>Email</label>
                <input
                  style={field}
                  type="email"
                  placeholder="you@company.com"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                />
              </div>
            </div>
          ) : (
            <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
              <div>
                <label>New password</label>
                <div style={{ position: "relative" }}>
                  <input
                    style={{ ...field, paddingRight: 88 }}
                    type={showPassword ? "text" : "password"}
                    placeholder="New password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
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
              <div>
                <label>Confirm password</label>
                <input
                  style={field}
                  type={showPassword ? "text" : "password"}
                  placeholder="Confirm password"
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.target.value)}
                />
              </div>
            </div>
          )}

          {message && (
            <div style={{ ...alertStyle, marginTop: 16, padding: "10px 14px", fontSize: 12, letterSpacing: "0.03em" }}>
              {message.text}
            </div>
          )}

          <button
            className="btn btn-primary"
            style={{ width: "100%", justifyContent: "center", padding: "13px 0", fontSize: 12, marginTop: 24, letterSpacing: "0.12em" }}
            onClick={isResetMode ? submitPassword : submitEmail}
            disabled={loading}
          >
            {loading ? "Working…" : isResetMode ? "Update Password →" : "Send Reset Link →"}
          </button>

          <div style={{ height: 1, background: "rgba(255,255,255,0.08)", margin: "24px 0" }} />

          <p style={{ textAlign: "center", fontSize: 12, color: "rgba(255,255,255,0.38)", letterSpacing: "0.04em" }}>
            Remembered your password?{" "}
            <Link to="/login" style={{ color: "#fff", textDecoration: "none", borderBottom: "1px solid rgba(255,255,255,0.3)", paddingBottom: 1 }}>
              Back to sign in
            </Link>
          </p>
        </div>
      </div>
    </div>
  )
}
