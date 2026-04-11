import React from "react";
import { Link, useLocation } from "react-router-dom"
import { useEffect, useState } from "react"

export default function Nav({ user, onLogout }) {
  const loc = useLocation()
  const [mobileOpen, setMobileOpen] = useState(false)
  const isGuest = !user || user.id === "guest"
  const plan = user?.plan ? user.plan.toUpperCase() : "STARTER"
  const isAdmin = (user?.role || "user").toLowerCase() === "admin"

  useEffect(() => {
    setMobileOpen(false)
  }, [loc.pathname])

  return (
    <nav className="nav">
      <Link to="/" className="nav-logo">
        <img src="/omnimate-logo.png" alt="OMNIMATE" className="nav-logo-img" />
        <span className="nav-logo-text">Lead<span>Scout</span></span>
      </Link>

      <button
        type="button"
        className="nav-mobile-toggle"
        aria-label={mobileOpen ? "Close navigation menu" : "Open navigation menu"}
        aria-expanded={mobileOpen}
        onClick={() => setMobileOpen((open) => !open)}
      >
        <span />
        <span />
        <span />
      </button>

      <div className={`nav-links ${mobileOpen ? "nav-links-open" : ""}`}>
        {isGuest ? (
          <>
            <Link to="/pricing" className={`nav-link ${loc.pathname === "/pricing" ? "active" : ""}`}>Pricing</Link>
            <Link to="/login"   className="btn btn-ghost" style={{ padding: "6px 16px" }}>Sign in</Link>
            <Link to="/signup"  className="btn btn-primary" style={{ padding: "6px 18px" }}>Sign up</Link>
          </>
        ) : (
          <>
            <Link to="/dashboard" className={`nav-link ${loc.pathname === "/dashboard" ? "active" : ""}`}>Dashboard</Link>
            <Link to="/leads" className={`nav-link ${loc.pathname === "/leads" ? "active" : ""}`}>My Leads</Link>
            <Link to="/profile" className={`nav-link ${loc.pathname === "/profile" ? "active" : ""}`}>Profile</Link>
            {isAdmin && (
              <Link to="/admin" className={`nav-link ${loc.pathname === "/admin" ? "active" : ""}`}>Admin</Link>
            )}
            <span className="badge badge-cyan nav-plan" style={{ marginLeft: 8 }}>{plan}</span>
            <span className="nav-user-name" style={{ fontSize: 12, color: "var(--text-secondary)", paddingLeft: 8 }}>{user.name}</span>
            <div className="nav-divider" style={{ width: 1, height: 20, background: "var(--border)", margin: "0 8px" }} />
            <button className="btn btn-ghost" style={{ padding: "6px 14px" }} onClick={onLogout}>Sign out</button>
          </>
        )}
      </div>
    </nav>
  )
}
