import React from "react";
import { useEffect, useState } from "react"
import { Navigate, Route, BrowserRouter as Router, Routes } from "react-router-dom"
import AdminPage from "./pages/AdminPage"
import DashboardPage from "./pages/DashboardPage"
import HomePage from "./pages/HomePage"
import LeadsPage from "./pages/LeadsPage"
import LoginPage from "./pages/LoginPage"
import ProfilePage from "./pages/ProfilePage"
import ResetPasswordPage from "./pages/ResetPasswordPage"
import SignupPage from "./pages/SignupPage"
import PricingPage from "./pages/PricingPage"
import { clearAuth, getStoredUser, getToken, isTokenExpired, setStoredUser, setToken, tryRefreshToken } from "./lib/auth"
import { apiUrl, getApiHeaders } from "./lib/api"

export default function App() {
  const [user, setUser] = useState(null)
  const [authReady, setAuthReady] = useState(false)

  const syncAdminEmail = (email) => {
    if (typeof window === "undefined") return
    if (email) {
      window.localStorage.setItem("user_email", email)
    } else {
      window.localStorage.removeItem("user_email")
    }
  }

  useEffect(() => {
    const token = getToken()
    const storedUser = getStoredUser()
    if (!token && !storedUser) {
      setAuthReady(true)
      return
    }
    if (!token || !storedUser) {
      clearAuth()
      setUser(null)
      syncAdminEmail("")
      setAuthReady(true)
      return
    }
    if (isTokenExpired(token)) {
      clearAuth()
      setUser(null)
      syncAdminEmail("")
      setAuthReady(true)
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
        setAuthReady(true)
      })
      .catch(() => {
        clearAuth()
        setUser(null)
        syncAdminEmail("")
        setAuthReady(true)
      })
  }, [])

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

  const login = ({ token, user: userData }) => {
    setToken(token)
    setStoredUser(userData)
    setUser(userData)
    syncAdminEmail(userData?.email || "")
  }

  const logout = () => {
    clearAuth()
    setUser(null)
  }

  const updateUser = (userData) => {
    setStoredUser(userData)
    setUser(userData)
    syncAdminEmail(userData?.email || "")
  }

  const leadscoutEntry = () => {
    if (!authReady) return <div className="page" style={{ minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center", color: "var(--text-secondary)" }}>Restoring session…</div>
    if (!user) return <Navigate to="/login" />
    return <Navigate to="/dashboard" />
  }

  const protectedArea = (element) => {
    if (!authReady) return <div className="page" style={{ minHeight: "100vh", display: "flex", alignItems: "center", justifyContent: "center", color: "var(--text-secondary)" }}>Restoring session…</div>
    if (!user) return <Navigate to="/login" />
    return element
  }

  return (
    <Router>
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/login"    element={<LoginPage  onLogin={login} />} />
        <Route path="/reset-password" element={<ResetPasswordPage />} />
        <Route path="/signup"   element={<SignupPage onLogin={login} />} />
        <Route path="/pricing"  element={<PricingPage user={user} onPlanSelected={updateUser} />} />
        <Route path="/leadscout" element={leadscoutEntry()} />
        <Route path="/dashboard" element={protectedArea(<DashboardPage user={user} onLogout={logout} />)} />
        <Route path="/leads" element={protectedArea(<LeadsPage user={user} onLogout={logout} />)} />
        <Route path="/profile"  element={protectedArea(<ProfilePage user={user} onLogout={logout} />)} />
        <Route path="/admin"    element={protectedArea(<AdminPage user={user} onLogout={logout} />)} />
      </Routes>
    </Router>
  )
}
