function cleanBaseUrl(value = "") {
  return String(value || "").trim().replace(/\/+$/, "")
}

export const API = cleanBaseUrl(import.meta.env.VITE_API_URL)
export const WS = cleanBaseUrl(import.meta.env.VITE_WS_URL)

const DEFAULT_HEADERS = {
  "Content-Type": "application/json",
  "ngrok-skip-browser-warning": "true",
}

export function getApiHeaders(headers = {}) {
  return {
    ...DEFAULT_HEADERS,
    ...headers,
  }
}

export function apiUrl(path = "") {
  if (/^https?:\/\//.test(path)) return path
  if (!API) throw new Error("Missing VITE_API_URL")

  let normalizedPath = path.startsWith("/") ? path : `/${path}`
  if (normalizedPath === "/api") normalizedPath = ""
  if (normalizedPath.startsWith("/api/")) normalizedPath = normalizedPath.slice(4)

  return `${API}${normalizedPath}`
}

export function wsUrl(path = "") {
  if (/^wss?:\/\//.test(path)) return path
  if (!WS) throw new Error("Missing VITE_WS_URL")

  let normalizedPath = path.startsWith("/") ? path : `/${path}`
  if (normalizedPath === "/api") normalizedPath = ""
  if (normalizedPath.startsWith("/api/")) normalizedPath = normalizedPath.slice(4)

  return `${WS}${normalizedPath}`
}
