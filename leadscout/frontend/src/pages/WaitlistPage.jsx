import { Link, useSearchParams } from "react-router-dom"
import { useState } from "react"
import SparklesBg from "../components/SparklesBg"

export default function WaitlistPage() {
  const [searchParams] = useSearchParams()
  const msg = searchParams.get("msg") || "Access is currently limited. Join the waitlist."
  const [email, setEmail] = useState("")
  const [status, setStatus] = useState("idle") // idle, loading, success, error

  const submit = async () => {
    if (!email || !email.includes("@")) return
    setStatus("loading")
    try {
      const res = await fetch("/api/waitlist", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email })
      })
      if (!res.ok) throw new Error("Failed")
      setStatus("success")
    } catch (e) {
      setStatus("error")
    }
  }

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
            {msg}
          </p>
          {status === "success" ? (
            <div style={{ padding: "16px", border: "1px solid rgba(255,255,255,0.2)", color: "#fff", fontSize: 13, letterSpacing: "0.04em" }}>
              Successfully added to waitlist! We'll notify you soon.
            </div>
          ) : (
            <>
              <input
                type="email"
                placeholder="Enter your email address"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                style={{ width: "100%", background: "#000", border: "1px solid rgba(255,255,255,0.18)", color: "#fff", padding: "13px 16px", fontSize: 13, outline: "none", marginBottom: 12, fontFamily: "var(--font-body)", borderRadius: 0 }}
              />
              <button onClick={submit} disabled={status === "loading"} className="btn btn-primary" style={{ display: "block", width: "100%", padding: "13px 0", fontSize: 12, letterSpacing: "0.12em", cursor: status === "loading" ? "not-allowed" : "pointer", border: "none" }}>
                {status === "loading" ? "Joining..." : "Join Waitlist →"}
              </button>
              {status === "error" && (
                <p style={{ color: "rgba(255,100,100,0.9)", fontSize: 11, marginTop: 12 }}>Failed to join waitlist. Try again later.</p>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  )
}
