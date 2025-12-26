from typing import Dict, List


def match_rule(event_path: str, rules: Dict[str, List[str]]) -> List[str]:
    """
    Match event path against rule map.

    event_path:
      /Company Home/Courses/Real Estate/Module1/file.png

    rule key:
      Module1/file.png
    """

    relative_path = _extract_relative_path(event_path)
    if not relative_path:
        return []

    matched: List[str] = []

    for rule_path, tags in rules.items():
        if _normalize(rule_path) == _normalize(relative_path):
            matched.extend(tags)

    return list(dict.fromkeys(matched))



def _extract_relative_path(event_path: str) -> str | None:
    """
    Extract path relative to COURSE folder.

    /Company Home/Courses/Real Estate/Module1/file.png
    → Module1/file.png
    """

    parts = [p for p in event_path.strip("/").split("/") if p]

    if parts and parts[0] == "Company Home":
        parts = parts[1:]

    if len(parts) < 3:
        return None

    return "/".join(parts[2:])


def _normalize(path: str) -> str:
    """
    Normalize paths for matching.
    Keep spaces, be case-sensitive by design.
    """
    return path.strip().lstrip("/")
