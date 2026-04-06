const WAITLIST_CONTEXT_KEY = "ls_waitlist_context"

function getStorage() {
  if (typeof window === "undefined") return null
  return window.sessionStorage
}

export function redirectToWaitlist(context = { reason: "access_restricted" }) {
  const storage = getStorage()
  if (storage) {
    storage.setItem(WAITLIST_CONTEXT_KEY, JSON.stringify(context))
  }
  window.location.assign("/waitlist")
}

export function consumeWaitlistContext() {
  const storage = getStorage()
  if (!storage) return null

  const raw = storage.getItem(WAITLIST_CONTEXT_KEY)
  if (!raw) return null

  storage.removeItem(WAITLIST_CONTEXT_KEY)

  try {
    return JSON.parse(raw)
  } catch {
    return null
  }
}
