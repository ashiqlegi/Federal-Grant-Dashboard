import requests

BASE_URL = "https://api.usaspending.gov/api/v2"

#fetch limit
FETCH_LIMIT = 100

#Extracting rewards from the USASpending API
def extract_awards():
    payload  = {
       "filters": {
            "award_type_codes": ["A", "B", "C", "D"],
            "time_period": [
                {"start_date": "2024-01-01", "end_date": "2024-12-31"}
            ]
        },
        "fields": [
            "Award ID",
            "Recipient Name",
            "Award Amount",
            "Awarding Agency",
            "Award Type",
            "Start Date",
            "End Date",
            "Description"
        ],
        "page": 1,
        "limit": FETCH_LIMIT,
        "sort": "Award Amount",
        "order": "desc" 
    }       

    url = BASE_URL + "/search/spending_by_award/"

    print(f"Calling API : {url}")   

    #Post request
    response = requests.post(url, json=payload, timeout = 30)

    response.raise_for_status()

    data = response.json()

    results = data.get("results", [])
    print(f"Extracted {len(results)} records from API.")
    return results


#test
if __name__ == "__main__":
    records = extract_awards()

    if records :
        print("\nFirst record :")
        for key, value in records[0].items():
            print(f" {key} : {value}")
