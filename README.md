🏡 Airbnb Analytics — dbt + Snowflake + Azure

📘 Overview
This project demonstrates a **modern data engineering pipeline** using **dbt**, **Snowflake**, and **Azure Data Lake Storage (ADLS)**.  
It transforms raw Airbnb data into analytics-ready models that power insights on **revenue, occupancy, pricing trends, and superhost performance**.

🧱 Architecture
The pipeline follows a **three-layer dbt structure**:

| Layer | Purpose | Example Models |
|--------|----------|----------------|
| **Staging** | Clean and standardize raw data from Snowflake staging schema | `stg_hosts`, `stg_listings`, `stg_bookings` |
| **Intermediate** | Join and enrich staging data for analytics | `int_listings_hosts`, `int_bookings_enriched` |
| **Marts** | Aggregate and calculate KPIs for dashboards | `booking_summary`, `city_performance`, `superhost_conversion`, `monthly_trends` |

---

🔗 Lineage Diagram
Include the lineage diagram image here:

```
![Airbnb dbt Project Lineage](path/to/lineage-diagram.png)
```

This visual shows how raw data flows from **staging → intermediate → marts**, ensuring transparency and traceability.

---

💡 Business Rules Implemented
- ✅ **Revenue Calculation Macro** — `booking_amount + cleaning_fee + service_fee`
- ✅ **Booking Status Validation** — Only `confirmed` bookings contribute to KPIs
- ✅ **Superhost Performance** — Compare conversion and response rates
- ✅ **Pricing & Occupancy Trends** — City-level and monthly analytics
- ✅ **Data Quality Tests** — `unique`, `not_null`, `relationships`, `accepted_values`
- ✅ **Referential Integrity** — Hosts ↔ Listings ↔ Bookings consistency

---

🧩 dbt Features Used
- **Sources & refs** for lineage tracking  
- **Macros** for reusable business logic  
- **Schema tests** for data validation  
- **Materializations** (`view`, `table`, `ephemeral`) for performance optimization  
- **Documentation site** (`dbt docs generate`) for interactive lineage and metadata  

---

 📊 Example KPIs
| Metric | Description |
|---------|--------------|
| **Total Revenue** | Sum of booking, cleaning, and service fees |
| **Average Booking Value** | Mean revenue per booking |
| **Occupancy Rate** | Nights booked ÷ available nights |
| **Superhost Conversion** | Bookings per superhost vs regular host |
| **Monthly Growth** | Revenue and booking trends by month |

---

🚀 How to Run
```bash
# Activate environment
.venv/Scripts/activate

# Run dbt models
dbt run

# Test data quality
dbt test

# Generate documentation
dbt docs generate && dbt docs serve
```


