import { Link, useNavigate } from "react-router-dom"
import { useEffect, useState } from "react"
import SparklesBg from "../components/SparklesBg"
import { apiUrl, getApiHeaders } from "../lib/api"
import { supabase } from "../lib/supabase"
import { consumeWaitlistContext } from "../lib/waitlist"

const FEEDBACK = {
  pending: {
    tone: "success",
    title: "Early access pending",
    body: "You're signed in and on the waitlist. We'll let you know as soon as your access is approved."
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
  },
  linkSent: {
    tone: "neutral",
    title: "Check your inbox",
    body: "We sent a sign-in link to your email. Open it to continue."
  },
  approved: {
    tone: "success",
    title: "Access approved",
    body: "You're approved for LeadScout. Redirecting now."
  },
  config: {
    tone: "error",
    title: "Auth is unavailable",
    body: "Supabase environment variables are missing for this deployment."
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
  const navigate = useNavigate()
  const [authMode, setAuthMode] = useState("signup")
  const [email, setEmail] = useState("")
  const [status, setStatus] = useState("idle")
  const [feedback, setFeedback] = useState(null)
  const [context, setContext] = useState(null)
  const [sessionEmail, setSessionEmail] = useState("")

  useEffect(() => {
    setContext(consumeWaitlistContext())
  }, [])

  useEffect(() => {
    if (!supabase) {
      setFeedback(FEEDBACK.config)
      return
    }

    let mounted = true

    supabase.auth.getSession().then(({ data }) => {
      if (!mounted) return
      const currentEmail = data.session?.user?.email?.trim().toLowerCase() || ""
      setSessionEmail(currentEmail)
      if (currentEmail) {
        setEmail(currentEmail)
      }
    })

    const { data: listener } = supabase.auth.onAuthStateChange((_event, session) => {
      const currentEmail = session?.user?.email?.trim().toLowerCase() || ""
      setSessionEmail(currentEmail)
      if (currentEmail) {
        setEmail(currentEmail)
      }
    })

    return () => {
      mounted = false
      listener.subscription.unsubscribe()
    }
  }, [])

  useEffect(() => {
    if (!sessionEmail) return

    let cancelled = false

    const syncAccess = async () => {
      setStatus("loading")
      try {
        const checkRes = await fetch(apiUrl(`/waitlist/check?email=${encodeURIComponent(sessionEmail)}`), {
          headers: getApiHeaders(),
        })
        const checkData = await checkRes.json().catch(() => null)
        if (!checkRes.ok) throw new Error(checkData?.detail || "Failed")

        if (checkData?.access) {
          if (!cancelled) {
            setStatus("approved")
            setFeedback(FEEDBACK.approved)
            window.localStorage.setItem("user_email", sessionEmail)
            setTimeout(() => navigate("/leadscout"), 500)
          }
          return
        }

        const joinRes = await fetch(apiUrl("/waitlist"), {
          method: "POST",
          headers: getApiHeaders(),
          body: JSON.stringify({ email: sessionEmail }),
        })
        const joinData = await joinRes.json().catch(() => null)

        if (!joinRes.ok) {
          throw new Error(joinData?.detail || joinData?.message || "Failed")
        }

        if (!cancelled) {
          setStatus(joinData?.ok === false ? "duplicate" : "pending")
          setFeedback(joinData?.ok === false ? FEEDBACK.duplicate : FEEDBACK.pending)
        }
      } catch {
        if (!cancelled) {
          setStatus("error")
          setFeedback(FEEDBACK.error)
        }
      }
    }

    syncAccess()

    return () => {
      cancelled = true
    }
  }, [navigate, sessionEmail])

  const submit = async () => {
    const normalizedEmail = email.trim().toLowerCase()

    if (!normalizedEmail || !normalizedEmail.includes("@")) {
      setStatus("idle")
      setFeedback(FEEDBACK.invalid)
      return
    }

    if (!supabase) {
      setStatus("error")
      setFeedback(FEEDBACK.config)
      return
    }

    setStatus("loading")
    setFeedback(null)

    try {
      const redirectTo = `${window.location.origin}/waitlist`
      const { error } = await supabase.auth.signInWithOtp({
        email: normalizedEmail,
        options: { emailRedirectTo: redirectTo }
      })
      if (error) throw error
      setStatus("link_sent")
      setFeedback(FEEDBACK.linkSent)
    } catch {
      setStatus("error")
      setFeedback(FEEDBACK.error)
    }
  }

  const signOut = async () => {
    if (!supabase) return
    await supabase.auth.signOut()
    window.localStorage.removeItem("user_email")
    setSessionEmail("")
    setStatus("idle")
    setFeedback(null)
    setEmail("")
  }

  const alertStyle = feedback ? ALERT_STYLES[feedback.tone] : null
  const showAccessNotice = context?.reason === "access_restricted"
  const isSignedIn = Boolean(sessionEmail)

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
            Access is currently limited while we roll out in stages. Sign in with your email to join the waitlist and keep your early access session active across refreshes.
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
            disabled={isSignedIn}
            onChange={(e) => {
              setEmail(e.target.value)
              if (feedback && status !== "loading") setFeedback(null)
            }}
            style={{ width: "100%", background: "#000", border: "1px solid rgba(255,255,255,0.18)", color: "#fff", padding: "13px 16px", fontSize: 13, outline: "none", marginBottom: 12, fontFamily: "var(--font-body)", borderRadius: 0, opacity: isSignedIn ? 0.72 : 1 }}
          />
          {!isSignedIn && (
            <div style={{ display: "flex", gap: 0, marginBottom: 12 }}>
              {["signup", "signin"].map((mode) => (
                <button
                  key={mode}
                  onClick={() => setAuthMode(mode)}
                  style={{
                    flex: 1,
                    padding: "10px 0",
                    fontSize: 11,
                    letterSpacing: "0.12em",
                    textTransform: "uppercase",
                    background: authMode === mode ? "rgba(255,255,255,0.04)" : "#010308",
                    color: authMode === mode ? "#fff" : "rgba(255,255,255,0.55)",
                    border: "1px solid rgba(255,255,255,0.12)",
                    cursor: "pointer",
                    fontFamily: "var(--font-display)",
                  }}
                >
                  {mode === "signup" ? "Sign Up" : "Sign In"}
                </button>
              ))}
            </div>
          )}
          <button onClick={submit} disabled={status === "loading" || isSignedIn} className="btn btn-primary" style={{ display: "block", width: "100%", padding: "13px 0", fontSize: 12, letterSpacing: "0.12em", cursor: status === "loading" || isSignedIn ? "not-allowed" : "pointer", border: "none" }}>
            {status === "loading" ? "Checking..." : isSignedIn ? "Signed In" : authMode === "signup" ? "Create Access Link →" : "Send Sign-In Link →"}
          </button>
          {isSignedIn && (
            <button onClick={signOut} className="btn btn-ghost" style={{ display: "block", width: "100%", padding: "13px 0", fontSize: 12, letterSpacing: "0.12em", border: "none", marginTop: 10 }}>
              Sign Out
            </button>
          )}
          <p style={{ color: "rgba(255,255,255,0.32)", fontSize: 11, marginTop: 12, lineHeight: 1.6 }}>
            {isSignedIn ? `Signed in as ${sessionEmail}.` : "We'll only use your email for access updates and product onboarding."}
          </p>
        </div>
      </div>
    </div>
  )
}
