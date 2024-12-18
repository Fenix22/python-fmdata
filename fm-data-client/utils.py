from typing import Dict


def clean_none(data: Dict) -> Dict:
    # remove keys with empty values
    return {k: v for k, v in data.items() if v is not None}
