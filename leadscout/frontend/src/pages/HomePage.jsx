import React from "react";
import { Link } from "react-router-dom"
import Nav from "../components/Nav"
import SparklesBg from "../components/SparklesBg"

const STATS = [
  { value: "140K+", label: "Leads per profession" },
  { value: "50+",   label: "Countries covered" },
  { value: "15+",   label: "Data points per lead" },
  { value: "100%",  label: "Auto-enriched" },
]

const FEATURES = [
  { icon: "◈", title: "Global Coverage",  desc: "India, UAE, UK, USA, Europe — every city split into granular areas." },
  { icon: "◉", title: "Auto Enrichment",  desc: "Owner names, emails, WhatsApp, LinkedIn from 5+ sources automatically." },
  { icon: "◈", title: "Dev Intelligence", desc: "Detects chatbot and mobile-friendliness. Flags every business to pitch." },
  { icon: "◉", title: "Zero Duplicates",  desc: "Persistent memory across all runs. Same business never collected twice." },
  { icon: "◈", title: "Any Niche",        desc: "Dentist, gym, lawyer — one word change. Works for any profession." },
  { icon: "◉", title: "Live Dashboard",   desc: "Watch leads appear in real time with progress tracking and CSV download." },
]

export default function HomePage() {
  return (
    <div className="page">
      <SparklesBg />
      <Nav />

      {/* ── Hero ──────────────────────────────────────────── */}
      <section style={{
        minHeight: "100vh",
        display: "flex", flexDirection: "column",
        alignItems: "center", justifyContent: "center",
        textAlign: "center", padding: "120px 24px 80px",
      }}>
        <div className="stagger" style={{ maxWidth: 820 }}>

          {/* OMNIMATE logo — decorative hero element */}
          <div style={{ marginBottom: 40, display: "inline-flex", flexDirection: "column", alignItems: "center", gap: 0 }}>
            <img
              src="/omnimate-logo.png"
              alt="OMNIMATE"
              style={{ height: 88, width: "auto", opacity: 0.92 }}
            />
            <div style={{
              marginTop: 10,
              fontSize: 9, fontWeight: 700, letterSpacing: "0.28em",
              textTransform: "uppercase", color: "rgba(255,255,255,0.28)",
              fontFamily: "var(--font-mono)",
            }}>
              Intelligence Platform
            </div>
          </div>

          {/* Live pill */}
          <div style={{ marginBottom: 28 }}>
            <span className="stat-pill">
              <span className="dot" /> Live intelligence engine
            </span>
          </div>

          {/* Headline */}
          <h1 style={{
            fontSize: "clamp(38px, 5.5vw, 72px)",
            fontWeight: 800,
            lineHeight: 1.0,
            letterSpacing: "-0.04em",
            marginBottom: 24,
            color: "#FFFFFF",
          }}>
            Find every lead.<br />
            <span style={{ color: "rgba(255,255,255,0.45)" }}>
              Own every market.
            </span>
          </h1>

          <p style={{
            fontSize: 17,
            color: "rgba(255,255,255,0.5)",
            lineHeight: 1.75,
            maxWidth: 520,
            margin: "0 auto 44px",
            letterSpacing: "0.01em",
          }}>
            The most advanced Google Maps lead scraper ever built.
            Auto-enriched with owner details, social media, and dev opportunity scoring.
          </p>

          <div style={{ display: "flex", gap: 12, justifyContent: "center" }}>
            <Link to="/signup" className="btn btn-primary" style={{ padding: "14px 36px", fontSize: 12 }}>
              Start scraping →
            </Link>
            <Link to="/pricing" className="btn btn-ghost" style={{ padding: "14px 36px", fontSize: 12 }}>
              See pricing
            </Link>
          </div>
        </div>

        {/* Stats bar */}
        <div style={{
          display: "grid", gridTemplateColumns: "repeat(4,1fr)",
          marginTop: 80, maxWidth: 700, width: "100%",
          border: "1px solid var(--border)",
        }}>
          {STATS.map((s, i) => (
            <div key={i} style={{
              background: "var(--bg-card)",
              padding: "22px 16px", textAlign: "center",
              borderRight: i < 3 ? "1px solid var(--border)" : "none",
            }}>
              <div style={{
                fontSize: "clamp(22px,3vw,30px)", fontWeight: 800,
                color: "#FFFFFF", letterSpacing: "-0.02em", marginBottom: 5,
                fontFamily: "var(--font-display)",
              }}>
                {s.value}
              </div>
              <div style={{
                fontSize: 10, color: "rgba(255,255,255,0.35)",
                textTransform: "uppercase", letterSpacing: "0.12em",
                fontFamily: "var(--font-mono)",
              }}>
                {s.label}
              </div>
            </div>
          ))}
        </div>
      </section>

      {/* ── Features ──────────────────────────────────────── */}
      <section id="features" style={{ padding: "80px 24px", maxWidth: 1100, margin: "0 auto" }}>
        <div style={{ textAlign: "center", marginBottom: 60 }}>
          <p style={{
            fontSize: 10, letterSpacing: "0.2em", textTransform: "uppercase",
            color: "rgba(255,255,255,0.3)", marginBottom: 16,
            fontFamily: "var(--font-mono)",
          }}>
            Capabilities
          </p>
          <h2 style={{
            fontSize: "clamp(32px,5vw,52px)", fontWeight: 800,
            letterSpacing: "-0.03em", color: "#FFFFFF",
          }}>
            Built different.
          </h2>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(300px,1fr))", gap: 1 }}>
          {FEATURES.map((f, i) => (
            <div key={i} className="card"
              style={{ borderRadius: 0, borderColor: "rgba(255,255,255,0.08)" }}
              onMouseEnter={e => e.currentTarget.style.borderColor = "rgba(255,255,255,0.25)"}
              onMouseLeave={e => e.currentTarget.style.borderColor = "rgba(255,255,255,0.08)"}
            >
              <div style={{
                width: 36, height: 36,
                border: "1px solid rgba(255,255,255,0.2)",
                display: "flex", alignItems: "center", justifyContent: "center",
                fontSize: 16, color: "#FFFFFF", marginBottom: 18,
              }}>
                {f.icon}
              </div>
              <h3 style={{
                fontSize: 13, fontWeight: 700, marginBottom: 10,
                letterSpacing: "0.06em", textTransform: "uppercase", color: "#FFFFFF",
              }}>
                {f.title}
              </h3>
              <p style={{ fontSize: 13, color: "rgba(255,255,255,0.45)", lineHeight: 1.65 }}>
                {f.desc}
              </p>
            </div>
          ))}
        </div>
      </section>

      {/* ── CTA ───────────────────────────────────────────── */}
      <section style={{
        textAlign: "center", padding: "80px 24px 120px",
        maxWidth: 600, margin: "0 auto",
      }}>
        <h2 style={{
          fontSize: "clamp(28px,4vw,44px)", fontWeight: 800,
          letterSpacing: "-0.03em", marginBottom: 16, color: "#FFFFFF",
        }}>
          Ready to collect<br />
          <span style={{ color: "rgba(255,255,255,0.4)" }}>140,000 leads?</span>
        </h2>
        <p style={{ color: "rgba(255,255,255,0.45)", marginBottom: 36, fontSize: 15 }}>
          Every business. Every city. Every niche. Enriched with owner details.
        </p>
        <Link to="/signup" className="btn btn-primary" style={{ padding: "14px 44px", fontSize: 12 }}>
          Get started →
        </Link>
      </section>

      {/* ── Footer ────────────────────────────────────────── */}
      <footer style={{
        borderTop: "1px solid var(--border)",
        padding: "24px 40px",
        display: "flex", justifyContent: "space-between", alignItems: "center",
        color: "rgba(255,255,255,0.25)", fontSize: 11,
        letterSpacing: "0.08em", textTransform: "uppercase",
        fontFamily: "var(--font-mono)",
      }}>
        <span>LeadScout — Universal lead intelligence</span>
        <span>Powered by OMNIMATE</span>
      </footer>
    </div>
  )
}
