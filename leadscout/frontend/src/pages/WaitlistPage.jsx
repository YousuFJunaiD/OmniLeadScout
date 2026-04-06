import { Link } from "react-router-dom"
import { useEffect, useState } from "react"
import SparklesBg from "../components/SparklesBg"
import { consumeWaitlistContext } from "../lib/waitlist"

const FEEDBACK = {
  success: {
    tone: "success",
    title: "You're on the list",
    body: "Thanks for joining. We'll reach out as soon as access opens up."
  },
  duplicate: {
    tone: "neutral",
    title: "You're already on the waitlist",
    body: "This email is already registered. We'll notify you when access is available."
  },
  error: {
    tone: "error",
    title: "We couldn't add you right now",
    body: "Please try again in a moment."
  },
  invalid: {
    tone: "error",
    title: "Enter a valid email",
    body: "Use a working email address so we can contact you when access opens."
  }
}

const ALERT_STYLES = {
  success: {
    border: "1px solid rgba(97, 219, 165, 0.28)",
    background: "rgba(97, 219, 165, 0.08)",
    color: "rgba(220,255,235,0.92)"
  },
  neutral: {
    border: "1px solid rgba(255,255,255,0.16)",
    background: "rgba(255,255,255,0.04)",
    color: "rgba(255,255,255,0.82)"
  },
  error: {
    border: "1px solid rgba(255,110,110,0.28)",
    background: "rgba(255,110,110,0.08)",
    color: "rgba(255,185,185,0.96)"
  }
}

export default function WaitlistPage() {
  const [email, setEmail] = useState("")
  const [status, setStatus] = useState("idle")
  const [feedback, setFeedback] = useState(null)
  const [context, setContext] = useState(null)

  useEffect(() => {
    setContext(consumeWaitlistContext())
  }, [])

  const submit = async () => {
    const normalizedEmail = email.trim().toLowerCase()

    if (!normalizedEmail || !normalizedEmail.includes("@")) {
      setStatus("idle")
      setFeedback(FEEDBACK.invalid)
      return
    }

    setStatus("loading")
    setFeedback(null)

    try {
      const res = await fetch("/api/waitlist", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email: normalizedEmail })
      })

      const data = await res.json().catch(() => null)

      if (!res.ok) {
        throw new Error(data?.detail || data?.message || "Failed")
      }

      if (data?.ok === false && data?.message === "Already joined") {
        setStatus("duplicate")
        setFeedback(FEEDBACK.duplicate)
        return
      }

      setStatus("success")
      setFeedback(FEEDBACK.success)
      setEmail(normalizedEmail)
    } catch {
      setStatus("error")
      setFeedback(FEEDBACK.error)
    }
  }

  const alertStyle = feedback ? ALERT_STYLES[feedback.tone] : null
  const showAccessNotice = context?.reason === "access_restricted"

  return (
    <div className="page" style={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "100vh", background: "#000" }}>
      <SparklesBg />
      <div style={{ width: "100%", maxWidth: 420, padding: "0 24px", zIndex: 1, textAlign: "center" }} className="anim-fade-up">
        {/* Logo */}
        <Link to="/" style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 8, marginBottom: 44, textDecoration: "none" }}>
          <img src="/omnimate-logo.png" alt="OMNIMATE" style={{ height: 56, width: "auto", opacity: 0.9 }} />
          <span style={{ fontSize: 10, letterSpacing: "0.24em", textTransform: "uppercase", color: "rgba(255,255,255,0.3)", fontFamily: "var(--font-mono)" }}>
            LeadScout
          </span>
        </Link>

        {/* Message */}
        <div style={{ border: "1px solid rgba(255,255,255,0.14)", background: "#0A0A0A", padding: "36px 32px" }}>
          <h2 style={{ fontSize: 20, fontWeight: 800, letterSpacing: "0.04em", textTransform: "uppercase", color: "#fff", marginBottom: 16 }}>
            Waitlist Active
          </h2>
          <p style={{ fontSize: 13, color: "rgba(255,255,255,0.5)", lineHeight: 1.6, marginBottom: 24, letterSpacing: "0.02em" }}>
            Access is currently limited while we roll out in stages. Join the waitlist and we&apos;ll let you know when your account can be activated.
          </p>

          {showAccessNotice && (
            <div style={{ ...ALERT_STYLES.neutral, padding: "14px 16px", textAlign: "left", fontSize: 12, lineHeight: 1.6, marginBottom: 16 }}>
              <div style={{ fontSize: 11, letterSpacing: "0.16em", textTransform: "uppercase", marginBottom: 6, color: "rgba(255,255,255,0.6)" }}>
                Access Restricted
              </div>
              New accounts are being reviewed before access is enabled. Join the waitlist below and we&apos;ll contact you when your invite is ready.
            </div>
          )}

          {feedback && (
            <div style={{ ...alertStyle, padding: "14px 16px", textAlign: "left", marginBottom: 16 }}>
              <div style={{ fontSize: 11, letterSpacing: "0.16em", textTransform: "uppercase", marginBottom: 6 }}>
                {feedback.title}
              </div>
              <div style={{ fontSize: 12, lineHeight: 1.6 }}>
                {feedback.body}
              </div>
            </div>
          )}

          <input
            type="email"
            placeholder="Enter your email address"
            value={email}
            onChange={(e) => {
              setEmail(e.target.value)
              if (feedback && status !== "loading") setFeedback(null)
            }}
            style={{ width: "100%", background: "#000", border: "1px solid rgba(255,255,255,0.18)", color: "#fff", padding: "13px 16px", fontSize: 13, outline: "none", marginBottom: 12, fontFamily: "var(--font-body)", borderRadius: 0 }}
          />
          <button onClick={submit} disabled={status === "loading"} className="btn btn-primary" style={{ display: "block", width: "100%", padding: "13px 0", fontSize: 12, letterSpacing: "0.12em", cursor: status === "loading" ? "not-allowed" : "pointer", border: "none" }}>
            {status === "loading" ? "Joining..." : status === "success" ? "Joined" : "Join Waitlist →"}
          </button>
          <p style={{ color: "rgba(255,255,255,0.32)", fontSize: 11, marginTop: 12, lineHeight: 1.6 }}>
            We&apos;ll only use your email for access updates and product onboarding.
          </p>
        </div>
      </div>
    </div>
  )
}
