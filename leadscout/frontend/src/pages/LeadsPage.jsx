import React from "react";
import { useEffect, useMemo, useState } from "react"
import { useNavigate } from "react-router-dom"
import Nav from "../components/Nav"
import SparklesBg from "../components/SparklesBg"
import { authFetch } from "../lib/auth"



function toCsv(rows) {
  if (!rows.length) return ""
  const headers = Object.keys(rows[0])
  const escape = (value) => `"${String(value ?? "").replace(/"/g, '""')}"`
  return [
    headers.join(","),
    ...rows.map((row) => headers.map((header) => escape(row[header])).join(",")),
  ].join("\n")
}

export default function LeadsPage({ user, onLogout }) {
  const navigate = useNavigate()
  const [leads, setLeads] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState("")
  const [search, setSearch] = useState("")
  const [source, setSource] = useState("")
  const [websiteStatus, setWebsiteStatus] = useState("")

  const loadLeads = async () => {
    setLoading(true)
    setError("")
    const params = new URLSearchParams()
    if (search.trim()) params.set("search", search.trim())
    if (source) params.set("source", source)
    if (websiteStatus) params.set("website_status", websiteStatus)

    const res = await authFetch(`/user/leads?${params.toString()}`, {}, () => navigate("/login"))
    if (!res.ok) {
      setError("Unable to load leads right now.")
      setLoading(false)
      return
    }
    const data = await res.json()
    setLeads(data.leads || [])
    setLoading(false)
  }

  useEffect(() => {
    loadLeads()
  }, [source, websiteStatus])

  const filtered = useMemo(() => leads, [leads])

  const exportCsv = () => {
    const csv = toCsv(filtered)
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" })
    const url = URL.createObjectURL(blob)
    const a = document.createElement("a")
    a.href = url
    a.download = `leadscout_all_leads_${Date.now()}.csv`
    a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <div className="page">
      <SparklesBg />
      <Nav user={user} onLogout={onLogout} />
      <div style={{ paddingTop: 64 }}>
        <div className="leads-page-shell" style={{ maxWidth: 1280, margin: "0 auto", padding: "36px 24px" }}>
          <div className="card" style={{ marginBottom: 16 }}>
            <div className="leads-header" style={{ display: "flex", justifyContent: "space-between", gap: 16, alignItems: "end", flexWrap: "wrap" }}>
              <div>
                <p style={{ fontSize: 10, letterSpacing: "0.1em", textTransform: "uppercase", color: "var(--text-muted)", marginBottom: 8 }}>My Leads</p>
                <h1 style={{ fontSize: 28, fontWeight: 800, letterSpacing: "-0.02em" }}>Lead Library</h1>
              </div>
              <button className="btn btn-primary" onClick={exportCsv} disabled={!filtered.length}>Export All CSV</button>
            </div>
            <div className="leads-filters-grid" style={{ display: "grid", gridTemplateColumns: "2fr 1fr 1fr auto", gap: 12, marginTop: 18 }}>
              <input
                placeholder="Search by name or city"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && loadLeads()}
              />
              <select value={source} onChange={(e) => setSource(e.target.value)}>
                <option value="">All sources</option>
                <option value="google_maps">Google Maps</option>
                <option value="justdial">JustDial</option>
                <option value="indiamart">IndiaMart</option>
              </select>
              <select value={websiteStatus} onChange={(e) => setWebsiteStatus(e.target.value)}>
                <option value="">All website statuses</option>
                <option value="no_website">No website</option>
                <option value="social_only">Social only</option>
                <option value="minimal">Minimal</option>
                <option value="full">Full</option>
                <option value="unreachable">Unreachable</option>
              </select>
              <button className="btn btn-ghost" onClick={loadLeads}>Apply</button>
            </div>
          </div>

          <div className="card" style={{ padding: 0, overflow: "hidden" }}>
            <div style={{ padding: "16px 20px", borderBottom: "1px solid var(--border)", display: "flex", justifyContent: "space-between" }}>
              <span style={{ fontSize: 13, fontWeight: 600 }}>All Scraped Leads</span>
              <span style={{ fontSize: 11, color: "var(--text-muted)" }}>{filtered.length} rows</span>
            </div>
            <div className="table-wrap">
              {loading ? (
                <div style={{ padding: 40, textAlign: "center", color: "var(--text-muted)" }}>Loading leads...</div>
              ) : error ? (
                <div style={{ padding: 40, textAlign: "center", color: "var(--accent-red)" }}>{error}</div>
              ) : filtered.length === 0 ? (
                <div style={{ padding: 40, textAlign: "center", color: "var(--text-muted)" }}>
                  {search || source || websiteStatus ? "No leads found for the selected filters." : "No leads saved yet."}
                </div>
              ) : (
                <>
                  <div className="mobile-only lead-mobile-list">
                    {filtered.map((lead) => (
                      <div key={`mobile-${lead.id}`} className="lead-mobile-card">
                        <div style={{ fontWeight: 600, color: "var(--text-primary)" }}>{lead.name || "—"}</div>
                        <div className="lead-mobile-meta">
                          <div>City: {lead.city || "—"}</div>
                          <div>Phone: {lead.phone || "—"}</div>
                          <div>Email: {lead.email || "—"}</div>
                          <div>Source: {lead.source || "—"}</div>
                          <div>Status: {lead.website_status || "—"}</div>
                        </div>
                        <div style={{ marginTop: 12 }}>
                          {lead.website ? (
                            <a href={lead.website} target="_blank" rel="noreferrer" className="badge badge-green" style={{ textDecoration: "none" }}>
                              Visit ↗
                            </a>
                          ) : (
                            <span className="badge badge-red">No Website</span>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                  <table className="data-table desktop-only">
                    <thead>
                      <tr>
                        <th>Name</th>
                        <th>City</th>
                        <th>Phone</th>
                        <th>Email</th>
                        <th>Source</th>
                        <th>Website</th>
                        <th>Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {filtered.map((lead) => (
                        <tr key={lead.id}>
                          <td>{lead.name || "—"}</td>
                          <td>{lead.city || "—"}</td>
                          <td>{lead.phone || "—"}</td>
                          <td>{lead.email || "—"}</td>
                          <td>{lead.source || "—"}</td>
                          <td>
                            {lead.website ? (
                              <a href={lead.website} target="_blank" rel="noreferrer" className="badge badge-green" style={{ textDecoration: "none" }}>
                                Visit ↗
                              </a>
                            ) : (
                              <span className="badge badge-red">None</span>
                            )}
                          </td>
                          <td>{lead.website_status || "—"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
