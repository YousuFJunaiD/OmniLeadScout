import { apiUrl, getApiHeaders } from "./api"
import { toErrorString } from "./errors"

const TOKEN_KEY = "ls_token"
const USER_KEY = "ls_user"
const FORCED_LOGOUT_KEY = "ls_forced_logout_message"

function getStorage() {
  if (typeof window === "undefined") return null
  const local = window.localStorage
  const session = window.sessionStorage
  if (!local.getItem(TOKEN_KEY) && session.getItem(TOKEN_KEY)) {
    local.setItem(TOKEN_KEY, session.getItem(TOKEN_KEY))
  }
  if (!local.getItem(USER_KEY) && session.getItem(USER_KEY)) {
    local.setItem(USER_KEY, session.getItem(USER_KEY))
  }
  return local
}

export function getToken() {
  return getStorage()?.getItem(TOKEN_KEY) || null
}

export function setToken(token) {
  if (token) getStorage()?.setItem(TOKEN_KEY, token)
}

export function clearToken() {
  getStorage()?.removeItem(TOKEN_KEY)
  if (typeof window !== "undefined") {
    window.sessionStorage.removeItem(TOKEN_KEY)
  }
}

export function getAuthHeaders(extra = {}) {
  const token = getToken()
  return {
    ...getApiHeaders(extra),
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
  }
}

export function getStoredUser() {
  const raw = getStorage()?.getItem(USER_KEY)
  if (!raw) return null
  try {
    return JSON.parse(raw)
  } catch {
    return null
  }
}

export function setStoredUser(user) {
  if (user) {
    const safeUser = { 
      id: user.id, 
      email: String(user.email || "").trim().toLowerCase(),
      name: user.name || user.full_name, 
      role: String(user.role || "user").trim().toLowerCase(),
      plan: user.plan,
      enterprise_access: Boolean(user.enterprise_access),
      enterprise_access_reason: String(user.enterprise_access_reason || ""),
    }
    getStorage()?.setItem(USER_KEY, JSON.stringify(safeUser))
    if (typeof window !== "undefined") {
      window.sessionStorage.setItem(USER_KEY, JSON.stringify(safeUser))
    }
  }
}

export function clearAuth() {
  clearToken()
  getStorage()?.removeItem(USER_KEY)
  if (typeof window !== "undefined") {
    window.sessionStorage.removeItem(USER_KEY)
    window.localStorage.removeItem("user_email")
    Object.keys(window.localStorage).forEach((key) => {
      if (key.startsWith("ls_active_job_")) {
        window.localStorage.removeItem(key)
      }
    })
  }
}

export function setForcedLogoutMessage(message) {
  if (typeof window === "undefined") return
  if (message) {
    window.sessionStorage.setItem(FORCED_LOGOUT_KEY, message)
  } else {
    window.sessionStorage.removeItem(FORCED_LOGOUT_KEY)
  }
}

export function consumeForcedLogoutMessage() {
  if (typeof window === "undefined") return ""
  const message = window.sessionStorage.getItem(FORCED_LOGOUT_KEY) || ""
  window.sessionStorage.removeItem(FORCED_LOGOUT_KEY)
  return message
}

async function readUnauthorizedMessage(response) {
  try {
    const clone = response.clone()
    const contentType = clone.headers.get("content-type") || ""
    if (contentType.includes("application/json")) {
      const data = await clone.json()
      return toErrorString(data?.detail || data?.error || data?.message || data)
    }
    return await clone.text()
  } catch {
    return ""
  }
}

export function canDownloadCsv(user) {
  const role = String(user?.role || "user").trim().toLowerCase()
  if (role === "admin") return true
  const plan = String(user?.plan || "starter").trim().toLowerCase()
  return ["pro", "growth", "team"].includes(plan)
}

export function isTokenExpired(token) {
  if (!token) return true
  try {
    const payload = JSON.parse(atob(token.split(".")[1]))
    const exp = Number(payload?.exp || 0)
    if (!exp) return false
    return Date.now() >= exp * 1000
  } catch {
    return true
  }
}

export async function tryRefreshToken() {
  const token = getToken()
  if (!token) return null
  const response = await fetch(apiUrl("/auth/refresh"), {
    method: "POST",
    headers: getAuthHeaders(),
  })
  if (!response.ok) {
    const message = await readUnauthorizedMessage(response)
    if (String(message).toLowerCase().includes("another device") || String(message).toLowerCase().includes("session invalid")) {
      setForcedLogoutMessage("You were signed out because your account was used on another device.")
    }
    clearAuth()
    return null
  }
  const data = await response.json()
  const nextToken = data?.token ?? data?.data?.token ?? null
  const nextUser = data?.user ?? data?.data?.user ?? null
  if (nextToken) setToken(nextToken)
  if (nextUser) setStoredUser(nextUser)
  return { ...data, token: nextToken, user: nextUser }
}

export async function authFetch(url, options = {}, onUnauthorized) {
  const headers = getAuthHeaders(options.headers || {})
  const response = await fetch(apiUrl(url), { ...options, headers })
  if (response.status === 401) {
    const message = await readUnauthorizedMessage(response)
    if (String(message).toLowerCase().includes("another device") || String(message).toLowerCase().includes("session invalid")) {
      setForcedLogoutMessage("You were signed out because your account was used on another device.")
    }
    clearAuth()
    if (onUnauthorized) onUnauthorized(message)
  }
  return response
}
