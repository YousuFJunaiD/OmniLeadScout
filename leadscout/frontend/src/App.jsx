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



export default function App() {
  const [user, setUser] = useState(null)

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
        syncAdminEmail("")
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
    syncAdminEmail("")
  }

  const updateUser = (userData) => {
    setStoredUser(userData)
    setUser(userData)
    syncAdminEmail(userData?.email || "")
  }

  return (
    <Router>
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/login"    element={<LoginPage  onLogin={login} />} />
        <Route path="/signup"   element={<SignupPage onLogin={login} />} />
        <Route path="/pricing"  element={<PricingPage user={user} onPlanSelected={updateUser} />} />
        <Route path="/dashboard" element={user ? (user.plan ? <DashboardPage user={user} onLogout={logout} /> : <Navigate to="/pricing" />) : <Navigate to="/login" />} />
        <Route path="/leads" element={user ? (user.plan ? <LeadsPage user={user} onLogout={logout} /> : <Navigate to="/pricing" />) : <Navigate to="/login" />} />
        <Route path="/profile"  element={user ? (user.plan ? <ProfilePage user={user} onLogout={logout} /> : <Navigate to="/pricing" />) : <Navigate to="/login" />} />
        <Route path="/waitlist" element={<WaitlistPage />} />
        <Route path="/admin"    element={<AdminPage user={user} onLogout={logout} />} />
      </Routes>
    </Router>
  )
}
