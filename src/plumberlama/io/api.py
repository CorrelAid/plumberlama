def make_headers(api_token: str) -> dict[str, str]:
    """Create HTTP headers for LamaPoll API requests."""
    return {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}


def preprocess_api_response(questions_raw: list[dict]) -> list[dict]:
    """Preprocess LamaPoll API response to fix inconsistencies.

    The LamaPoll API has inconsistent behavior where it returns empty lists []
    instead of the expected types. This function normalizes the response to match
    the Pydantic model expectations.

    Fixes applied:
    - range: [] → None (should be None for non-scale questions)
    - name: [] → {} (should be empty dict, not empty list)
    - labels: [[], ...] → [{}, ...] (list elements should be dicts)
    - items[].name: [] → {} (should be empty dict)
    """
    for question in questions_raw:
        for group in question.get("groups", []):
            # Fix range: [] -> None
            if group.get("range") == []:
                group["range"] = None

            # Fix name: [] -> {}
            if group.get("name") == []:
                group["name"] = {}

            # Fix labels: replace [] with {} in list
            if "labels" in group:
                group["labels"] = [lab if lab != [] else {} for lab in group["labels"]]

            # Fix items
            for item in group.get("items", []):
                if item.get("name") == []:
                    item["name"] = {}

    return questions_raw
