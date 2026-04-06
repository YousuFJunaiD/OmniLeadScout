export const API = import.meta.env.VITE_API_URL || "/api"

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
