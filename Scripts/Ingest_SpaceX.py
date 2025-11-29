import requests
import snowflake.connector

# SpaceX API
SPACEX_URL = "https://api.spacexdata.com/v4/launches"

# Snowflake connection (external browser auth)
conn = snowflake.connector.connect(
    user="MICHELLEGIBBONS143",
    account="UMEGPHF-OJ25583",
    authenticator="externalbrowser",
    role="ACCOUNTADMIN",
    warehouse="COMPUTE_WH",
    database="SPACEX_PROJECT",
    schema="BRONZE"
)
cur = conn.cursor()

# create table if it doesn't exist
cur.execute("""
CREATE TABLE IF NOT EXISTS SPACEX (
    SrcCreated TIMESTAMP,
    SrcLastModified TIMESTAMP,
    Payload VARIANT
)
""")

# fetch all launches
response = requests.get(SPACEX_URL)
response.raise_for_status()
data = response.json()

# get last loaded SrcLastModified for incremental load
cur.execute("SELECT MAX(SrcLastModified) FROM SPACEX")
last_loaded = cur.fetchone()[0]

# filter new or updated launches
new_data = [d for d in data if last_loaded is None or d["updated"] > last_loaded]

if not new_data:
    print("No new or updated launches to load.")
else:
    print(f"Loading {len(new_data)} new/updated launches...")

    for launch in new_data:
        cur.execute(
            "INSERT INTO SPACEX (SrcCreated, SrcLastModified, Payload) VALUES (%s, %s, %s)",
            (launch["created"], launch["updated"], launch)
        )

conn.commit()
cur.close()
conn.close()
print("Done.")
