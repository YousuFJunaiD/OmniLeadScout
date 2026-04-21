// Shared error-to-string helper.
//
// Backends return errors in several shapes:
//   - string: "Monthly lead limit reached"
//   - HTTPException: { detail: "Forbidden" }  or  { detail: { error, plan, limits, ... } }
//   - FastAPI validation: { detail: [ { loc, msg, type }, ... ] }
//   - Global handler wrap: { success: false, message, error, detail }
//
// toReadableError flattens all of these into a single human string without
// ever producing "[object Object]" or a raw JSON blob.

const NETWORK_PATTERNS = [
  "failed to fetch",
  "networkerror",
  "load failed",
  "cors",
]

const SESSION_PATTERNS = [
  "session invalid",
  "another device",
  "authorization",
]

export function toReadableError(value, fallback = "Something went wrong") {
  if (value == null || value === "") return fallback

  if (typeof value === "string") {
    const lower = value.toLowerCase()
    if (NETWORK_PATTERNS.some((pattern) => lower.includes(pattern))) {
      return "Connection issue. Retrying..."
    }
    if (SESSION_PATTERNS.some((pattern) => lower.includes(pattern))) {
      return "Your session has expired. Please sign in again."
    }
    if (lower.includes("internal server error")) {
      return "We couldn't complete this request. Please try again."
    }
    return value
  }

  if (Array.isArray(value)) {
    const items = value
      .map((item) => toReadableError(item?.msg || item?.message || item?.error || item, ""))
      .filter(Boolean)
    return items.length ? items.join(". ") : fallback
  }

  if (typeof value === "object") {
    const candidate =
      value.error ||
      value.message ||
      value.msg ||
      value.detail ||
      value.description
    if (candidate != null && candidate !== value) {
      return toReadableError(candidate, fallback)
    }
    return fallback
  }

  return fallback
}

// Flatten an unknown shape to a plain string for logical checks
// (e.g. "does this message contain 'another device'?"). Never returns
// "[object Object]" or a JSON string.
export function toErrorString(value) {
  if (value == null) return ""
  if (typeof value === "string") return value
  if (Array.isArray(value)) {
    return value.map((item) => toErrorString(item?.msg || item)).filter(Boolean).join(" ")
  }
  if (typeof value === "object") {
    return String(
      value.error ||
        value.message ||
        value.msg ||
        value.detail ||
        value.description ||
        ""
    )
  }
  return String(value)
}
