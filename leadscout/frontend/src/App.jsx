import React from "react";
import { useEffect, useState } from "react"
import { Navigate, Route, BrowserRouter as Router, Routes } from "react-router-dom"
import AdminPage from "./pages/AdminPage"
import DashboardPage from "./pages/DashboardPage"
import HomePage from "./pages/HomePage"
import LeadsPage from "./pages/LeadsPage"
import LoginPage from "./pages/LoginPage"
import ProfilePage from "./pages/ProfilePage"
import SignupPage from "./pages/SignupPage"
import PricingPage from "./pages/PricingPage"
import WaitlistPage from "./pages/WaitlistPage"
import { clearAuth, getStoredUser, getToken, isTokenExpired, setStoredUser, setToken, tryRefreshToken } from "./lib/auth"
import { apiUrl, getApiHeaders } from "./lib/api"
import { supabase } from "./lib/supabase"



export default function App() {
  const [user, setUser] = useState(null)
  const [supabaseEmail, setSupabaseEmail] = useState("")
  const [waitlistAccess, setWaitlistAccess] = useState("idle")

  const syncAdminEmail = (email) => {
    if (typeof window === "undefined") return
    if (email) {
      window.localStorage.setItem("user_email", email)
    } else {
      window.localStorage.removeItem("user_email")
    }
  }

  useEffect(() => {
    if (!supabase) return

    let mounted = true

    supabase.auth.getSession().then(({ data }) => {
      if (!mounted) return
      const email = data.session?.user?.email || ""
      setSupabaseEmail(email)
      syncAdminEmail(email)
    })

    const { data: listener } = supabase.auth.onAuthStateChange((_event, session) => {
      const email = session?.user?.email || ""
      setSupabaseEmail(email)
      syncAdminEmail(email || user?.email || "")
    })

    return () => {
      mounted = false
      listener.subscription.unsubscribe()
    }
  }, [user?.email])

  useEffect(() => {
    const token = getToken()
    const storedUser = getStoredUser()
    if (!token || !storedUser) return
    if (isTokenExpired(token)) {
      clearAuth()
      setUser(null)
      return
    }

    fetch(apiUrl("/auth/me"), {
      headers: getApiHeaders({ Authorization: `Bearer ${token}` }),
    })
      .then(async (res) => {
        if (!res.ok) throw new Error("unauthorized")
        const data = await res.json()
        setStoredUser(data.user)
        setUser(data.user)
        syncAdminEmail(data.user?.email || "")
      })
      .catch(() => {
        clearAuth()
        setUser(null)
        syncAdminEmail(supabaseEmail)
      })
  }, [supabaseEmail])

  useEffect(() => {
    if (!user) return
    const interval = setInterval(() => {
      tryRefreshToken()
        .then((data) => {
          if (data?.user) setUser(data.user)
        })
        .catch(() => {})
    }, 5 * 60 * 1000)
    return () => clearInterval(interval)
  }, [user])

  useEffect(() => {
    const email = user?.email || supabaseEmail
    if (!email) {
      setWaitlistAccess("idle")
      return
    }

    let cancelled = false
    setWaitlistAccess("loading")

    fetch(apiUrl(`/waitlist/check?email=${encodeURIComponent(email)}`), {
      headers: getApiHeaders(),
    })
      .then(async (res) => {
        if (!res.ok) throw new Error("waitlist check failed")
        const data = await res.json()
        if (!cancelled) {
          setWaitlistAccess(data?.access ? "allowed" : "blocked")
        }
      })
      .catch(() => {
        if (!cancelled) setWaitlistAccess("blocked")
      })

    return () => {
      cancelled = true
    }
  }, [user?.email, supabaseEmail])

  const login = ({ token, user: userData }) => {
    setToken(token)
    setStoredUser(userData)
    setUser(userData)
    syncAdminEmail(userData?.email || "")
  }

  const logout = () => {
    clearAuth()
    setUser(null)
    syncAdminEmail("")
  }

  const updateUser = (userData) => {
    setStoredUser(userData)
    setUser(userData)
    syncAdminEmail(userData?.email || "")
  }

  const leadscoutEntry = () => {
    const email = user?.email || supabaseEmail
    if (!email) return <Navigate to="/waitlist" />
    if (waitlistAccess === "loading") {
      return (
        <div className="page" style={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "100vh" }}>
          <div className="card" style={{ maxWidth: 420, width: "100%", textAlign: "center" }}>
            <p style={{ color: "var(--text-secondary)", fontSize: 13 }}>Checking access...</p>
          </div>
        </div>
      )
    }
    if (waitlistAccess === "blocked") return <Navigate to="/waitlist" />
    if (user) return user.plan ? <Navigate to="/dashboard" /> : <Navigate to="/pricing" />
    return <Navigate to="/signup" />
  }

  const protectedArea = (element) => {
    if (!user) return <Navigate to="/login" />
    if (!user.plan) return <Navigate to="/pricing" />
    if (waitlistAccess === "loading") {
      return (
        <div className="page" style={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "100vh" }}>
          <div className="card" style={{ maxWidth: 420, width: "100%", textAlign: "center" }}>
            <p style={{ color: "var(--text-secondary)", fontSize: 13 }}>Checking access...</p>
          </div>
        </div>
      )
    }
    if (waitlistAccess === "blocked") return <Navigate to="/waitlist" />
    return element
  }

  return (
    <Router>
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/login"    element={<LoginPage  onLogin={login} />} />
        <Route path="/signup"   element={<SignupPage onLogin={login} />} />
        <Route path="/pricing"  element={<PricingPage user={user} onPlanSelected={updateUser} />} />
        <Route path="/leadscout" element={leadscoutEntry()} />
        <Route path="/dashboard" element={protectedArea(<DashboardPage user={user} onLogout={logout} />)} />
        <Route path="/leads" element={protectedArea(<LeadsPage user={user} onLogout={logout} />)} />
        <Route path="/profile"  element={protectedArea(<ProfilePage user={user} onLogout={logout} />)} />
        <Route path="/waitlist" element={<WaitlistPage />} />
        <Route path="/admin"    element={<AdminPage user={user} onLogout={logout} />} />
      </Routes>
    </Router>
  )
}
