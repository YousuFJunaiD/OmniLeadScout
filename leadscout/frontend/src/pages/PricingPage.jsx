import React from "react";
import { useState } from "react"
import { Link, useNavigate } from "react-router-dom"
import Nav from "../components/Nav"
import SparklesBg from "../components/SparklesBg"
import { authFetch } from "../lib/auth"

// ── Data ──────────────────────────────────────���──────────────────────────────

const PLANS = [
  {
    id: "starter",
    name: "Starter",
    tagline: "Just getting started",
    monthly: 0,
    annual: 0,
    strike: null,
    popular: false,
    features: [
      { text: "100 leads / month",        ok: true  },
      { text: "3 searches / day",          ok: true  },
      { text: "Google Maps only",          ok: true  },
      { text: "CSV export",                ok: true  },
      { text: "Community support",         ok: true  },
      { text: "7-day data retention",      ok: true  },
      { text: "Phone numbers",             ok: false },
      { text: "Email extraction",          ok: false },
      { text: "Duplicate removal",         ok: false },
      { text: "Website status check",      ok: false },
    ],
    cta: "Start Free",
    ctaType: "link",
    ctaTo: "/dashboard",
  },
  {
    id: "pro",
    name: "Pro",
    tagline: "For serious prospectors",
    monthly: 2499,
    annual: 1874,
    strike: 3299,
    popular: false,
    features: [
      { text: "3,500 leads / month",       ok: true  },
      { text: "50 searches / day",          ok: true  },
      { text: "Google Maps + JustDial",     ok: true  },
      { text: "Phone numbers (partial)",    ok: true  },
      { text: "Email extraction",           ok: true  },
      { text: "Duplicate removal",          ok: true  },
      { text: "Website status check",       ok: true  },
      { text: "Priority support (24h)",     ok: true  },
      { text: "30-day data retention",      ok: true  },
      { text: "IndiaMart source",           ok: false },
      { text: "AI lead scoring",            ok: false },
      { text: "Scheduled scraping",         ok: false },
    ],
    cta: "Get Pro",
    ctaType: "payment",
  },
  {
    id: "growth",
    name: "Growth",
    tagline: "For agencies & power users",
    monthly: 5499,
    annual: 4124,
    strike: 7999,
    popular: true,
    features: [
      { text: "10,000 leads / month",                    ok: true },
      { text: "Unlimited searches",                       ok: true },
      { text: "Google Maps + JustDial + IndiaMart",       ok: true },
      { text: "Full phone numbers (88%+ verified)",       ok: true },
      { text: "AI lead scoring",                          ok: true },
      { text: "Scheduled scraping",                       ok: true },
      { text: "Multi-niche campaigns",                    ok: true },
      { text: "WhatsApp priority support",                ok: true },
      { text: "45-day data retention",                    ok: true },
    ],
    cta: "Start Growth",
    ctaType: "payment",
  },
  {
    id: "enterprise",
    name: "Enterprise",
    tagline: "Unlimited scale, your brand",
    monthly: null,
    annual: null,
    strike: null,
    popular: false,
    features: [
      { text: "100,000+ leads / month",      ok: true },
      { text: "Dedicated proxy pool",         ok: true },
      { text: "API access",                   ok: true },
      { text: "White-label dashboard",        ok: true },
      { text: "SLA guarantee",                ok: true },
      { text: "Dedicated account manager",    ok: true },
    ],
    cta: "Contact Sales",
    ctaType: "contact",
  },
]

const ADDONS = [
  { id: "leads",     name: "Extra 1,000 leads",           price: 299,  period: "one-time", plans: "All plans"  },
  { id: "retention", name: "Extended data retention",      price: 199,  period: "/mo",      plans: "All plans"  },
  { id: "excel",     name: "Excel / Google Sheets export", price: 399,  period: "/mo",      plans: "All plans"  },
  { id: "scoring",   name: "AI lead scoring",              price: 499,  period: "/mo",      plans: "Pro+"       },
  { id: "alerts",    name: "Real-time alerts",             price: 599,  period: "/mo",      plans: "Pro+"       },
  { id: "websource", name: "Web scraping source",          price: 699,  period: "/mo",      plans: "Pro+"       },
  { id: "crm",       name: "CRM integration",              price: 999,  period: "/mo",      plans: "Growth+"    },
  { id: "automation",name: "Automation workflows",         price: 1299, period: "/mo",      plans: "Growth+"    },
]

const FAQS = [
  {
    q: "Why Growth over Pro?",
    a: "Pro users hit the 3,500 lead cap by day 18 on average. Growth gives you 10,000 leads, unlimited searches, all 3 platforms, and AI scoring — for agencies serious about building pipeline.",
  },
  {
    q: "Can I try before committing?",
    a: "Yes. Start free on the Starter plan and upgrade anytime. Paid plans come with a 7-day money-back guarantee — no questions asked.",
  },
  {
    q: "What counts as a lead?",
    a: "Each unique business listing = 1 lead. Duplicates that get removed do not count against your monthly quota.",
  },
  {
    q: "How accurate are phone numbers?",
    a: "Pro gives 65–70% coverage with partial numbers. Growth delivers 88%+ verified full numbers, sourced and cross-referenced from multiple directories.",
  },
  {
    q: "Do add-ons roll over?",
    a: "Monthly add-ons cancel anytime with no penalty. One-time lead top-ups expire at the end of the current billing month.",
  },
  {
    q: "What is the Enterprise minimum?",
    a: "Enterprise is designed for teams needing 10,000+ leads per month, white-labelling, or direct API integration. Contact sales for a custom quote.",
  },
]

// ── Helpers ───────────────────────────────���───────────────────────────────────

const fmt = (n) => n === null ? "Custom" : `₹${n.toLocaleString("en-IN")}`

// ── Component ────────────────────────────────────────────────────────────���────

const API = import.meta.env.VITE_API_URL || "http://127.0.0.1:8001"

const loadRazorpay = () =>
  new Promise((resolve) => {
    if (window.Razorpay) {
      resolve(true)
      return
    }
    const script = document.createElement("script")
    script.src = "https://checkout.razorpay.com/v1/checkout.js"
    script.onload = () => resolve(true)
    script.onerror = () => resolve(false)
    document.body.appendChild(script)
  })

export default function PricingPage({ user, onPlanSelected }) {
  const nav = useNavigate()
  const [billing, setBilling]           = useState("monthly")
  const [selected, setSelected]         = useState(new Set())
  const [openFaq, setOpenFaq]           = useState(null)
  const [showContact, setShowContact]   = useState(false)
  const [contactSent, setContactSent]   = useState(false)
  const [contact, setContact]           = useState({ name: "", email: "", company: "", message: "" })
  const [selectingPlan, setSelectingPlan] = useState("")
  const [planError, setPlanError] = useState("")
  const isSelectingPlan = Boolean(selectingPlan)

  const price = (plan) => billing === "annual" && plan.annual !== null ? plan.annual : plan.monthly

  const toggleAddon = (id) => {
    setSelected(prev => {
      const next = new Set(prev)
      next.has(id) ? next.delete(id) : next.add(id)
      return next
    })
  }

  const addonTotal = [...selected].reduce((sum, id) => {
    const a = ADDONS.find(x => x.id === id)
    return sum + (a ? a.price : 0)
  }, 0)

  const handlePlanCta = async (plan) => {
    if (isSelectingPlan) return
    if (!user) {
      nav("/signup")
      return
    }
    setPlanError("")
    setSelectingPlan(plan.id)
    try {
      if (plan.id === "enterprise") {
        setShowContact(true)
        setTimeout(() => document.getElementById("contact-section")?.scrollIntoView({ behavior: "smooth" }), 50)
        return
      }

      if (plan.id === "starter") {
        const res = await authFetch(
          `${API}/user/select-plan`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ plan: plan.id }),
          },
          () => nav("/login")
        )
        const data = await res.json()
        if (!res.ok) throw new Error(data.detail || data.error || "Failed to select plan, try again")
        onPlanSelected?.(data.user)
        nav("/dashboard")
        return
      }

      const ready = await loadRazorpay()
      if (!ready) throw new Error("Payment checkout failed to load")

      const orderRes = await authFetch(
        `${API}/payment/create-order`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ plan: plan.id }),
        },
        () => nav("/login")
      )
      const orderData = await orderRes.json()
      if (!orderRes.ok) throw new Error(orderData.detail || orderData.error || "Failed to create payment order")

      await new Promise((resolve, reject) => {
        const razorpay = new window.Razorpay({
          key: orderData.key_id,
          amount: orderData.amount,
          currency: "INR",
          name: "LeadScout",
          description: `${plan.name} plan`,
          order_id: orderData.order_id,
          handler: async (response) => {
            try {
              const verifyRes = await authFetch(
                `${API}/payment/verify`,
                {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify({
                    razorpay_order_id: response.razorpay_order_id,
                    razorpay_payment_id: response.razorpay_payment_id,
                    razorpay_signature: response.razorpay_signature,
                    plan: plan.id,
                  }),
                },
                () => nav("/login")
              )
              const verifyData = await verifyRes.json()
              if (!verifyRes.ok) throw new Error(verifyData.detail || verifyData.error || "Payment verification failed")
              onPlanSelected?.(verifyData.user)
              nav("/dashboard")
              resolve()
            } catch (error) {
              reject(error)
            }
          },
          prefill: {
            name: user.name || "",
            email: user.email || "",
          },
          theme: {
            color: "#ffffff",
          },
          modal: {
            ondismiss: () => reject(new Error("Payment cancelled")),
          },
        })
        razorpay.open()
      })
    } catch (error) {
      setPlanError("Failed to select plan, try again")
      console.error(error)
    } finally {
      setSelectingPlan("")
    }
  }

  const submitContact = (e) => {
    e.preventDefault()
    setContactSent(true)
  }

  // ── Styles ─────────────────────────────────────────────────────────────────

  const S = {
    page:       { background: "#000", minHeight: "100vh", color: "#fff" },
    fomo:       { background: "#0A0A0A", borderBottom: "1px solid rgba(255,255,255,0.1)", padding: "10px 24px", textAlign: "center", fontSize: 12, letterSpacing: "0.06em", color: "rgba(255,255,255,0.55)" },
    section:    { maxWidth: 1180, margin: "0 auto", padding: "0 24px" },
    label:      { fontSize: 9, fontWeight: 700, letterSpacing: "0.18em", textTransform: "uppercase", color: "rgba(255,255,255,0.3)", fontFamily: "var(--font-mono)" },
    h2:         { fontSize: "clamp(28px,4vw,42px)", fontWeight: 800, letterSpacing: "-0.03em", color: "#fff", lineHeight: 1.1 },
    planGrid:   { display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 0, border: "1px solid rgba(255,255,255,0.12)" },
    card:       (popular) => ({
      background:   popular ? "#0D0D0D" : "#000",
      borderRight:  "1px solid rgba(255,255,255,0.12)",
      padding:      "28px 24px 32px",
      position:     "relative",
      outline:      popular ? "1px solid rgba(255,255,255,0.55)" : "none",
      outlineOffset: popular ? "-1px" : 0,
    }),
    toggle:     (active) => ({
      padding: "8px 22px", fontSize: 11, fontWeight: 700,
      letterSpacing: "0.1em", textTransform: "uppercase",
      cursor: "pointer", border: "1px solid rgba(255,255,255,0.2)",
      background: active ? "#fff" : "transparent",
      color: active ? "#000" : "rgba(255,255,255,0.55)",
      transition: "all 0.15s",
      fontFamily: "var(--font-display)",
    }),
    addonCard:  (sel) => ({
      border: `1px solid ${sel ? "rgba(255,255,255,0.6)" : "rgba(255,255,255,0.1)"}`,
      background: sel ? "rgba(255,255,255,0.04)" : "#000",
      padding: "16px 18px",
      cursor: "pointer",
      transition: "all 0.15s",
    }),
    faqRow:     { borderBottom: "1px solid rgba(255,255,255,0.08)", cursor: "pointer" },
    input:      { background: "#000", border: "1px solid rgba(255,255,255,0.18)", borderRadius: 0, color: "#fff", padding: "11px 14px", fontSize: 13, outline: "none", width: "100%", fontFamily: "var(--font-body)", letterSpacing: "0.02em" },
  }

  return (
    <div style={S.page}>
      <SparklesBg />
      <Nav user={user} />

      {/* ── FOMO bar ───────────────────────────────────��─────────────────── */}
      <div style={{ ...S.fomo, paddingTop: 74 }}>
        <span style={{ marginRight: 8, opacity: 0.5 }}>▲</span>
        347 agencies upgraded to Growth this month — most said they wished they&apos;d done it sooner
      </div>

      {/* ── Hero ───────────────────���─────────────────────────────────────── */}
      <div style={{ ...S.section, paddingTop: 72, paddingBottom: 56, textAlign: "center" }}>
        <p style={{ ...S.label, marginBottom: 16 }}>Pricing</p>
        <h1 style={{ ...S.h2, fontSize: "clamp(32px,5vw,54px)", marginBottom: 16 }}>
          Simple, transparent pricing.
        </h1>
        <p style={{ fontSize: 15, color: "rgba(255,255,255,0.45)", marginBottom: 36, letterSpacing: "0.02em" }}>
          Choose your plan to finish setting up your account.
        </p>
        {planError && (
          <div style={{ maxWidth: 520, margin: "0 auto 24px", border: "1px solid rgba(255,100,100,0.25)", padding: "12px 16px", color: "rgba(255,120,120,0.95)", fontSize: 13 }}>
            {planError}
          </div>
        )}

        {/* Billing toggle */}
        <div style={{ display: "inline-flex", alignItems: "center", gap: 0 }}>
          <button style={S.toggle(billing === "monthly")} onClick={() => setBilling("monthly")}>Monthly</button>
          <button style={{ ...S.toggle(billing === "annual"), borderLeft: "none" }} onClick={() => setBilling("annual")}>
            Annual
            <span style={{ marginLeft: 8, fontSize: 9, letterSpacing: "0.1em", background: billing === "annual" ? "rgba(0,0,0,0.15)" : "rgba(255,255,255,0.12)", color: billing === "annual" ? "#000" : "rgba(255,255,255,0.7)", padding: "2px 7px" }}>
              SAVE 25%
            </span>
          </button>
        </div>
      </div>

      {/* ── Plans ────────────────────────────���───────────────────────────── */}
      <div style={S.section}>
        <div style={S.planGrid}>
          {PLANS.map((plan) => (
            <div key={plan.id} style={S.card(plan.popular)}>

              {/* Popular badge */}
              {plan.popular && (
                <div style={{ position: "absolute", top: -1, left: 0, right: 0, background: "#fff", color: "#000", textAlign: "center", padding: "4px 0", fontSize: 9, fontWeight: 800, letterSpacing: "0.18em", fontFamily: "var(--font-mono)" }}>
                  MOST POPULAR
                </div>
              )}

              <div style={{ paddingTop: plan.popular ? 20 : 0 }}>
                {/* Name + tagline */}
                <p style={{ ...S.label, marginBottom: 6 }}>{plan.name}</p>
                <p style={{ fontSize: 12, color: "rgba(255,255,255,0.35)", marginBottom: 20, letterSpacing: "0.04em" }}>{plan.tagline}</p>

                {/* Price */}
                <div style={{ marginBottom: 24 }}>
                  {plan.monthly === null ? (
                    <div style={{ fontSize: 32, fontWeight: 800, letterSpacing: "-0.03em", color: "#fff" }}>Custom</div>
                  ) : (
                    <>
                      <div style={{ display: "flex", alignItems: "baseline", gap: 6 }}>
                        {plan.strike && billing === "monthly" && (
                          <span style={{ fontSize: 14, color: "rgba(255,255,255,0.25)", textDecoration: "line-through", fontFamily: "var(--font-mono)" }}>
                            {fmt(plan.strike)}
                          </span>
                        )}
                        <span style={{ fontSize: 32, fontWeight: 800, letterSpacing: "-0.03em", color: "#fff", fontFamily: "var(--font-display)" }}>
                          {price(plan) === 0 ? "Free" : fmt(price(plan))}
                        </span>
                        {price(plan) > 0 && (
                          <span style={{ fontSize: 12, color: "rgba(255,255,255,0.35)" }}>/mo</span>
                        )}
                      </div>
                      {billing === "annual" && plan.annual > 0 && (
                        <p style={{ fontSize: 11, color: "rgba(255,255,255,0.35)", marginTop: 3, letterSpacing: "0.04em" }}>
                          {fmt(plan.annual * 12)} billed annually
                        </p>
                      )}
                    </>
                  )}
                </div>

                {/* CTA */}
                <button
                  onClick={() => handlePlanCta(plan)}
                  disabled={isSelectingPlan}
                  style={{
                    width: "100%", padding: "11px 0", fontSize: 11, fontWeight: 700,
                    letterSpacing: "0.12em", textTransform: "uppercase", cursor: "pointer",
                    border: plan.popular ? "none" : "1px solid rgba(255,255,255,0.3)",
                    background: plan.popular ? "#fff" : "transparent",
                    color: plan.popular ? "#000" : "#fff",
                    fontFamily: "var(--font-display)", marginBottom: 24, transition: "opacity 0.15s",
                  }}
                  onMouseEnter={e => { e.currentTarget.style.opacity = "0.8" }}
                  onMouseLeave={e => { e.currentTarget.style.opacity = "1" }}
                >
                  {selectingPlan === plan.id ? "Processing..." : `${plan.cta} →`}
                </button>

                {/* Feature list */}
                <div style={{ display: "flex", flexDirection: "column", gap: 9 }}>
                  {plan.features.map((f, fi) => (
                    <div key={fi} style={{ display: "flex", alignItems: "flex-start", gap: 10 }}>
                      <span style={{ fontSize: 10, color: f.ok ? "rgba(255,255,255,0.9)" : "rgba(255,255,255,0.2)", flexShrink: 0, marginTop: 1, fontFamily: "var(--font-mono)" }}>
                        {f.ok ? "✓" : "—"}
                      </span>
                      <span style={{ fontSize: 12, color: f.ok ? "rgba(255,255,255,0.7)" : "rgba(255,255,255,0.22)", letterSpacing: "0.02em", lineHeight: 1.45, textDecoration: f.ok ? "none" : "none" }}>
                        {f.text}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* ── Add-ons ───────────────────────────────────────────────────────── */}
      <div style={{ ...S.section, paddingTop: 80, paddingBottom: 20 }}>
        <div style={{ marginBottom: 36 }}>
          <p style={{ ...S.label, marginBottom: 12 }}>Power-ups</p>
          <h2 style={{ ...S.h2, fontSize: "clamp(22px,3vw,32px)", marginBottom: 8 }}>Power-ups for every plan</h2>
          <p style={{ fontSize: 13, color: "rgba(255,255,255,0.38)", letterSpacing: "0.04em" }}>
            Click to select. Prices added to your plan total.
          </p>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(260px,1fr))", gap: 1, background: "rgba(255,255,255,0.08)" }}>
          {ADDONS.map((addon) => {
            const sel = selected.has(addon.id)
            return (
              <div
                key={addon.id}
                style={{ ...S.addonCard(sel), background: sel ? "rgba(255,255,255,0.05)" : "#000" }}
                onClick={() => toggleAddon(addon.id)}
                role="checkbox"
                aria-checked={sel}
              >
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 8 }}>
                  <span style={{ fontSize: 12, fontWeight: 600, color: sel ? "#fff" : "rgba(255,255,255,0.65)", letterSpacing: "0.03em", lineHeight: 1.4 }}>
                    {addon.name}
                  </span>
                  <span style={{ fontSize: 11, width: 18, height: 18, border: `1px solid ${sel ? "#fff" : "rgba(255,255,255,0.25)"}`, display: "flex", alignItems: "center", justifyContent: "center", flexShrink: 0, color: sel ? "#000" : "transparent", background: sel ? "#fff" : "transparent", fontWeight: 800, marginLeft: 10 }}>
                    ✓
                  </span>
                </div>
                <div style={{ display: "flex", alignItems: "baseline", gap: 4 }}>
                  <span style={{ fontSize: 16, fontWeight: 800, color: sel ? "#fff" : "rgba(255,255,255,0.5)", fontFamily: "var(--font-display)" }}>
                    ₹{addon.price.toLocaleString("en-IN")}
                  </span>
                  <span style={{ fontSize: 10, color: "rgba(255,255,255,0.3)", letterSpacing: "0.06em" }}>{addon.period}</span>
                </div>
                <div style={{ marginTop: 6, fontSize: 10, color: "rgba(255,255,255,0.28)", letterSpacing: "0.08em", textTransform: "uppercase", fontFamily: "var(--font-mono)" }}>
                  {addon.plans}
                </div>
              </div>
            )
          })}
        </div>
      </div>

      {/* ── Enterprise contact form ───────────────────────────────────────── */}
      {showContact && (
        <div id="contact-section" style={{ ...S.section, paddingTop: 60, paddingBottom: 20 }}>
          <div style={{ border: "1px solid rgba(255,255,255,0.18)", padding: "40px 36px", maxWidth: 600 }}>
            <p style={{ ...S.label, marginBottom: 12 }}>Enterprise</p>
            <h2 style={{ fontSize: 24, fontWeight: 800, letterSpacing: "-0.02em", color: "#fff", marginBottom: 6 }}>Contact Sales</h2>
            <p style={{ fontSize: 13, color: "rgba(255,255,255,0.4)", marginBottom: 28, letterSpacing: "0.03em" }}>
              Tell us about your team. We&apos;ll get back within one business day.
            </p>

            {contactSent ? (
              <div style={{ border: "1px solid rgba(255,255,255,0.3)", padding: "20px 24px", textAlign: "center" }}>
                <div style={{ fontSize: 13, fontWeight: 700, letterSpacing: "0.08em", textTransform: "uppercase", color: "#fff", marginBottom: 6 }}>Message sent</div>
                <p style={{ fontSize: 12, color: "rgba(255,255,255,0.45)" }}>We&apos;ll be in touch within one business day.</p>
              </div>
            ) : (
              <form onSubmit={submitContact} style={{ display: "flex", flexDirection: "column", gap: 16 }}>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
                  <div>
                    <label>Name</label>
                    <input style={S.input} placeholder="Your name" value={contact.name} onChange={e => setContact(c => ({ ...c, name: e.target.value }))} required />
                  </div>
                  <div>
                    <label>Email</label>
                    <input style={S.input} type="email" placeholder="you@company.com" value={contact.email} onChange={e => setContact(c => ({ ...c, email: e.target.value }))} required />
                  </div>
                </div>
                <div>
                  <label>Company</label>
                  <input style={S.input} placeholder="Company name" value={contact.company} onChange={e => setContact(c => ({ ...c, company: e.target.value }))} />
                </div>
                <div>
                  <label>What do you need?</label>
                  <textarea style={{ ...S.input, height: 100, resize: "vertical" }} placeholder="Describe your use case, volume, and any specific requirements…" value={contact.message} onChange={e => setContact(c => ({ ...c, message: e.target.value }))} required />
                </div>
                <button type="submit" className="btn btn-primary" style={{ alignSelf: "flex-start", padding: "12px 28px", fontSize: 11 }}>
                  Send Message →
                </button>
              </form>
            )}
          </div>
        </div>
      )}

      {/* ── FAQ ───────────────────────────────────────────────────────────── */}
      <div style={{ ...S.section, paddingTop: 80, paddingBottom: 100 }}>
        <div style={{ marginBottom: 40 }}>
          <p style={{ ...S.label, marginBottom: 12 }}>FAQ</p>
          <h2 style={{ ...S.h2, fontSize: "clamp(22px,3vw,32px)" }}>Common questions</h2>
        </div>

        <div style={{ border: "1px solid rgba(255,255,255,0.1)", maxWidth: 780 }}>
          {FAQS.map((faq, i) => {
            const open = openFaq === i
            return (
              <div key={i} style={{ ...S.faqRow, borderBottom: i < FAQS.length - 1 ? "1px solid rgba(255,255,255,0.08)" : "none" }}>
                <button
                  onClick={() => setOpenFaq(open ? null : i)}
                  style={{ width: "100%", background: "none", border: "none", cursor: "pointer", padding: "20px 24px", display: "flex", justifyContent: "space-between", alignItems: "center", gap: 16, textAlign: "left" }}
                >
                  <span style={{ fontSize: 13, fontWeight: 700, letterSpacing: "0.04em", color: "#fff", lineHeight: 1.4 }}>{faq.q}</span>
                  <span style={{ fontSize: 16, color: "rgba(255,255,255,0.4)", flexShrink: 0, transform: open ? "rotate(45deg)" : "none", transition: "transform 0.2s" }}>+</span>
                </button>
                {open && (
                  <div style={{ padding: "0 24px 20px", fontSize: 13, color: "rgba(255,255,255,0.5)", lineHeight: 1.7, letterSpacing: "0.03em" }}>
                    {faq.a}
                  </div>
                )}
              </div>
            )
          })}
        </div>
      </div>

      {/* ── Footer ───────���───────────────────────────────────────────────── */}
      <footer style={{ borderTop: "1px solid rgba(255,255,255,0.08)", padding: "24px 40px", display: "flex", justifyContent: "space-between", alignItems: "center", color: "rgba(255,255,255,0.22)", fontSize: 11, letterSpacing: "0.08em", textTransform: "uppercase", fontFamily: "var(--font-mono)" }}>
        <span>LeadScout — Universal lead intelligence</span>
        <span>Powered by OMNIMATE</span>
      </footer>

      {/* ── Sticky add-ons bar ────────────────────────────────────────────── */}
      {selected.size > 0 && (
        <div style={{ position: "fixed", bottom: 0, left: 0, right: 0, zIndex: 200, background: "#fff", borderTop: "1px solid rgba(0,0,0,0.15)", padding: "14px 32px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <span style={{ fontSize: 13, color: "#000", fontWeight: 600, letterSpacing: "0.03em" }}>
            <strong>{selected.size}</strong> add-on{selected.size > 1 ? "s" : ""} selected
            &nbsp;·&nbsp;
            Total: <strong>₹{addonTotal.toLocaleString("en-IN")}</strong>
            <span style={{ fontSize: 11, fontWeight: 400, color: "rgba(0,0,0,0.5)", marginLeft: 2 }}>/mo</span>
          </span>
          <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
            <button
              onClick={() => setSelected(new Set())}
              style={{ fontSize: 11, background: "none", border: "1px solid rgba(0,0,0,0.2)", color: "rgba(0,0,0,0.5)", padding: "7px 14px", cursor: "pointer", letterSpacing: "0.08em", textTransform: "uppercase", fontFamily: "var(--font-display)", fontWeight: 600 }}
            >
              Clear
            </button>
            <Link
              to="/signup"
              style={{ fontSize: 11, background: "#000", color: "#fff", border: "1px solid #000", padding: "8px 20px", textDecoration: "none", letterSpacing: "0.1em", textTransform: "uppercase", fontFamily: "var(--font-display)", fontWeight: 700 }}
            >
              Get started →
            </Link>
          </div>
        </div>
      )}
    </div>
  )
}
