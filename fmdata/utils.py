from typing import Dict

blacklisted_fields_names = ["record_id", "mod_id", "portal_name", "table_occurrence", "model", "portal", "layout"]

def clean_none(data: Dict) -> Dict:
    # remove keys with empty values
    return {k: v for k, v in data.items() if v is not None}

def check_field_name(field_name: str) -> None:
    if field_name is None or len(field_name) == 0:
        raise ValueError("Field name cannot be empty.")

    if "__" in field_name:
        raise ValueError("Field name cannot contain '__'.")

    if field_name.startswith("_"):
        raise ValueError("Field name cannot start with '_'.")

    if field_name in blacklisted_fields_names:
        raise ValueError(f"Field name '{field_name}' is not allowed.")

