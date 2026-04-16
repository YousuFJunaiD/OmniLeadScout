const DEFAULT_API_BASE = "http://159.89.168.191:8000"
const DEFAULT_WS_BASE = "ws://159.89.168.191:8000"

export const API = import.meta.env.VITE_API_URL || DEFAULT_API_BASE
export const WS = import.meta.env.VITE_WS_URL || (
  /^https?:\/\//.test(API)
    ? API.replace(/^http:/, "ws:").replace(/^https:/, "wss:")
    : DEFAULT_WS_BASE
)

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

  let normalizedPath = path.startsWith("/") ? path : `/${path}`
  if (normalizedPath === "/api") normalizedPath = ""
  if (normalizedPath.startsWith("/api/")) normalizedPath = normalizedPath.slice(4)

  return `${API}${normalizedPath}`
}

export function wsUrl(path = "") {
  if (/^wss?:\/\//.test(path)) return path

  let normalizedPath = path.startsWith("/") ? path : `/${path}`
  if (normalizedPath === "/api") normalizedPath = ""
  if (normalizedPath.startsWith("/api/")) normalizedPath = normalizedPath.slice(4)

  return `${WS}${normalizedPath}`
}
