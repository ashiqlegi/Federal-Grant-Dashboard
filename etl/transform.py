#Function to clean and validate raw award records from the API before loading into the database.

def transform_awards(raw_records):

   #Empty List    
    cleaned = []

    quality_issues = {}

    for record in raw_records:
        award_id = _clean_text(record.get("Award ID"))
        recipient = _clean_text(record.get("Recipient Name"))
        agency = _clean_text(record.get("Awarding Agency"))
        award_type = _clean_text(record.get("Award Type"))
        description = _clean_text(record.get("Description"))
        start_date = _clean_date(record.get("Start Date"))
        end_date = _clean_date(record.get("End Date"))
        amount = _clean_amount(record.get("Award Amount"))

        #Error handling 
        if award_type is None:
            quality_issues["missing_award_type"] = (
                quality_issues.get("missing_award_type", 0) + 1
            )

        if description and "IGF" in description:
            quality_issues["internal_description"] = (
                quality_issues.get("internal_description",0) + 1
            )
        if recipient is None:
            quality_issues["missing_recipient"] = (
                quality_issues.get("missing_recipient", 0 ) + 1

            )



            #Building the cleaned record.
            #Only the essential API fields are included.
        cleaned_record = {
            "award_id" : award_id,
            "award_type" : award_type,
            "recipient_name" : recipient,
            "awarding_agency":  agency,
            "award_amount":     amount,
            "start_date":       start_date,
            "end_date":         end_date,
            "description":      description
        }
        cleaned.append(cleaned_record)
    quality_flags = [
        {"field": field, "issue": field.replace("_", " "), "count": count}
        for field, count in quality_issues.items()
    ]    

    print(f"Transformed {len(cleaned)} records")
    print(f"Quality flags found: {len(quality_flags)}")
    for flag in quality_flags:
        print(f"  - {flag['issue']}: {flag['count']} records")

    return cleaned, quality_flags

    #Small helper functions
def _clean_text(value):
    """Returns stripped text or None if empty."""
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned if cleaned else None

def _clean_amount(value):
    """Converts value to float or returns None if invalid."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def _clean_date(value):
    """Validates date format YYYY-MM-DD or returns None."""
    if value is None:
        return None
    value = str(value).strip()
    # Check it loosely matches YYYY-MM-DD
    if len(value) == 10 and value[4] == "-" and value[7] == "-":
        return value
    return None

#Direct testing
if __name__ == "__main__":
    # Import extract so we can test with real data
    from extract import extract_awards

    raw = extract_awards()
    cleaned, flags = transform_awards(raw)

    print("\nFirst cleaned record:")
    for key, value in cleaned[0].items():
        print(f"  {key}: {value}")