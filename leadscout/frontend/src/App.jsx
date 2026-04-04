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
import { clearAuth, getStoredUser, getToken, isTokenExpired, setStoredUser, setToken, tryRefreshToken } from "./lib/auth"

const API = import.meta.env.VITE_API_URL || "http://localhost:8000"

export default function App() {
  const [user, setUser] = useState(null)

  useEffect(() => {
    const token = getToken()
    const storedUser = getStoredUser()
    if (!token || !storedUser) return
    if (isTokenExpired(token)) {
      clearAuth()
      setUser(null)
      return
    }

    fetch(`${API}/auth/me`, {
      headers: { Authorization: `Bearer ${token}` },
    })
      .then(async (res) => {
        if (!res.ok) throw new Error("unauthorized")
        const data = await res.json()
        setStoredUser(data.user)
        setUser(data.user)
      })
      .catch(() => {
        clearAuth()
        setUser(null)
      })
  }, [])

  useEffect(() => {
    if (!user) return
    const interval = setInterval(() => {
      tryRefreshToken(API)
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
  }

  const logout = () => {
    clearAuth()
    setUser(null)
  }

  const updateUser = (userData) => {
    setStoredUser(userData)
    setUser(userData)
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
        <Route path="/admin"    element={user?.role === "admin" ? <AdminPage user={user} onLogout={logout} /> : <Navigate to={user?.plan ? "/dashboard" : "/pricing"} />} />
      </Routes>
    </Router>
  )
}
