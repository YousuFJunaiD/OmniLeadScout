# OmniLeadScout 🚀

## Overview

OmniLeadScout is a SaaS platform for automated lead generation using multiple data sources like Google Maps, JustDial, and IndiaMART.

---

## Features

* 🔐 Authentication (JWT-based)
* 💳 Razorpay payments (Pro & Growth plans)
* 🧠 Smart scraping (async + fast)
* 📊 Admin panel (user + system control)
* 📧 Email system (Resend integration)
* 📈 Plan enforcement (usage limits)

---

## Tech Stack

Frontend:

* React (Vite)

Backend:

* FastAPI (Python)

Database:

* Supabase (PostgreSQL)

Payments:

* Razorpay

Emails:

* Resend

---

## Project Structure

/backend → FastAPI server
/frontend → React app

---

## Setup (Local)

### Backend

```
cd backend
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload
```

### Frontend

```
cd frontend
npm install
npm run dev
```

---

## Environment Variables

Backend (.env):

* SUPABASE_URL
* SUPABASE_SERVICE_KEY
* DATABASE_URL
* JWT_SECRET
* RAZORPAY_KEY_ID
* RAZORPAY_KEY_SECRET
* RESEND_API_KEY

Frontend (.env):

* VITE_API_URL

---

## Roles

User:

* Limited scraping based on plan

Admin:

* Unlimited scraping
* Access to /admin panel
* Manage users and plans

---

## Plans

Starter:

* 100 leads/month
* Google Maps only

Pro:

* 3500 leads/month
* Maps + JustDial

Growth:

* 10000 leads/month
* All sources

---

## Important Notes

* Do NOT commit .env files
* Do NOT expose API keys
* Use test mode for Razorpay during development

---

## Status

✅ MVP Complete
🚀 Ready for deployment

---

## Contributors

Team OmniLeadScout
