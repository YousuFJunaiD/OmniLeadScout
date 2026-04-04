# LeadScout

## Security Notes

- Never commit `.env` files or real credentials.
- Use environment variables only. Example templates live in:
  - `leadscout/backend/.env.example`
  - `leadscout/frontend/.env.example`
- Rotate any secret that was ever committed or shared locally.
- For Supabase production deployments, enable RLS and apply the policies in [`leadscout/backend/supabase_security.sql`](leadscout/backend/supabase_security.sql).

# Google Maps Lead Scraper

Universal scraper — any niche, any city, exports clean CSV ready for Google Sheets.

---

## Folder Structure

```
maps-scraper/
├── scraper.py        ← Main script (run this)
├── config.py         ← ⭐ EDIT THIS — queries, niche, settings
├── utils.py          ← Helper functions (don't need to touch)
├── requirements.txt  ← Python dependencies
├── output/           ← CSV files saved here automatically
└── README.md
```

---

## Setup (One Time)

```bash
# 1. Create and activate virtual environment
python -m venv venv

# Windows:
venv\Scripts\activate

# Mac/Linux:
source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Install Playwright browser
playwright install chromium
```

---

## How to Use

### Step 1 — Open `config.py` and set your niche + queries

```python
# Set the output filename
NICHE = "dentists_mumbai"

# Add your search queries
SEARCH_QUERIES = [
    "dentist in Andheri",
    "dentist in Bandra",
    "dentist in Borivali",
    # add more areas...
]
```

### Step 2 — Run the scraper

```bash
python scraper.py
```

### Step 3 — Get your CSV

CSV is saved in the `output/` folder:
```
output/dentists_mumbai_20240315_1430.csv
```

---

## CSV Columns

| Column | Description |
|---|---|
| Name | Business name |
| Category | Type (Dentist, Gym, etc.) |
| Rating | Google rating (e.g. 4.7) |
| Reviews | Number of reviews |
| Phone | Clinic/business phone |
| Email | Extracted from website (if available) |
| Address | Full address |
| Website | Website URL (empty if not listed) |
| Website Status | "Present" or "Not Listed" |
| Maps URL | Direct Google Maps link |
| Search Query | Which query found this lead |

---

## Import into Google Sheets

1. Open Google Sheets
2. **File → Import → Upload**
3. Select the CSV from the `output/` folder
4. Choose "Comma" as separator
5. Click **Import Data**

---

## Examples for Different Niches

```python
# CA Firms in Delhi
NICHE = "ca_firms_delhi"
SEARCH_QUERIES = [
    "CA firm in Connaught Place",
    "chartered accountant in Lajpat Nagar",
    "CA office in Dwarka",
]

# Gyms in Bangalore
NICHE = "gyms_bangalore"
SEARCH_QUERIES = [
    "gym in Koramangala",
    "fitness center in Indiranagar",
    "gym in Whitefield",
]

# Restaurants in Hyderabad
NICHE = "restaurants_hyderabad"
SEARCH_QUERIES = [
    "restaurant in Jubilee Hills",
    "restaurant in Banjara Hills",
    "restaurant in Hitech City",
]
```

---

## Tips

- **Getting blocked?** Set `HEADLESS = False` and increase `DELAY_BETWEEN_LISTINGS = 3000`
- **More leads?** Add more area-wise queries. Each query = ~120 leads.
- **Duplicates?** The scraper deduplicates automatically before saving.
- **No website?** The `Website Status` column will show "Not Listed" — these are your warm leads to pitch web dev services to.

# Google Maps Lead Scraper

Universal scraper — any niche, any city, exports clean CSV ready for Google Sheets.

---

## Folder Structure

```
maps-scraper/
├── scraper.py        ← Main script (run this)
├── config.py         ← ⭐ EDIT THIS — queries, niche, settings
├── utils.py          ← Helper functions (don't need to touch)
├── requirements.txt  ← Python dependencies
├── output/           ← CSV files saved here automatically
└── README.md
```

---

## Setup (One Time)

```bash
# 1. Create and activate virtual environment
python -m venv venv

# Windows:
venv\Scripts\activate

# Mac/Linux:
source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Install Playwright browser
playwright install chromium
```

---

## How to Use

### Step 1 — Open `config.py` and set your niche + queries

```python
# Set the output filename
NICHE = "dentists_mumbai"

# Add your search queries
SEARCH_QUERIES = [
    "dentist in Andheri",
    "dentist in Bandra",
    "dentist in Borivali",
    # add more areas...
]
```

### Step 2 — Run the scraper

```bash
python scraper.py
```

### Step 3 — Get your CSV

CSV is saved in the `output/` folder:
```
output/dentists_mumbai_20240315_1430.csv
```

---

## CSV Columns

| Column | Description |
|---|---|
| Name | Business name |
| Category | Type (Dentist, Gym, etc.) |
| Rating | Google rating (e.g. 4.7) |
| Reviews | Number of reviews |
| Phone | Clinic/business phone |
| Email | Extracted from website (if available) |
| Address | Full address |
| Website | Website URL (empty if not listed) |
| Website Status | "Present" or "Not Listed" |
| Maps URL | Direct Google Maps link |
| Search Query | Which query found this lead |

---

## Import into Google Sheets

1. Open Google Sheets
2. **File → Import → Upload**
3. Select the CSV from the `output/` folder
4. Choose "Comma" as separator
5. Click **Import Data**

---

## Examples for Different Niches

```python
# CA Firms in Delhi
NICHE = "ca_firms_delhi"
SEARCH_QUERIES = [
    "CA firm in Connaught Place",
    "chartered accountant in Lajpat Nagar",
    "CA office in Dwarka",
]

# Gyms in Bangalore
NICHE = "gyms_bangalore"
SEARCH_QUERIES = [
    "gym in Koramangala",
    "fitness center in Indiranagar",
    "gym in Whitefield",
]

# Restaurants in Hyderabad
NICHE = "restaurants_hyderabad"
SEARCH_QUERIES = [
    "restaurant in Jubilee Hills",
    "restaurant in Banjara Hills",
    "restaurant in Hitech City",
]
```

---

## Tips

- **Getting blocked?** Set `HEADLESS = False` and increase `DELAY_BETWEEN_LISTINGS = 3000`
- **More leads?** Add more area-wise queries. Each query = ~120 leads.
- **Duplicates?** The scraper deduplicates automatically before saving.
- **No website?** The `Website Status` column will show "Not Listed" — these are your warm leads to pitch web dev services to.
