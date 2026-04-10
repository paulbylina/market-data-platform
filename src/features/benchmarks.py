from __future__ import annotations


SECTOR_ETF_MAP: dict[str, str] = {
    "communication_services": "XLC",
    "consumer_discretionary": "XLY",
    "consumer_staples": "XLP",
    "energy": "XLE",
    "financials": "XLF",
    "health_care": "XLV",
    "industrials": "XLI",
    "materials": "XLB",
    "real_estate": "XLRE",
    "technology": "XLK",
    "utilities": "XLU",
}


_SECTOR_ALIASES: dict[str, str] = {
    "communication services": "communication_services",
    "communication_services": "communication_services",
    "communications": "communication_services",
    "comms": "communication_services",

    "consumer discretionary": "consumer_discretionary",
    "consumer_discretionary": "consumer_discretionary",

    "consumer staples": "consumer_staples",
    "consumer_staples": "consumer_staples",

    "energy": "energy",

    "financials": "financials",
    "financial": "financials",

    "health care": "health_care",
    "healthcare": "health_care",
    "health_care": "health_care",

    "industrials": "industrials",
    "industrial": "industrials",

    "materials": "materials",
    "material": "materials",

    "real estate": "real_estate",
    "real_estate": "real_estate",

    "technology": "technology",
    "tech": "technology",

    "utilities": "utilities",
    "utility": "utilities",
}


def normalize_sector_name(sector_name: str) -> str:
    """
    Normalize a user- or vendor-provided sector label into the canonical key
    used by SECTOR_ETF_MAP.

    Examples
    --------
    >>> normalize_sector_name("Health Care")
    'health_care'
    >>> normalize_sector_name("tech")
    'technology'
    """
    if not isinstance(sector_name, str):
        raise TypeError("sector_name must be a string")

    cleaned = sector_name.strip().lower().replace("-", " ")
    cleaned = " ".join(cleaned.split())

    if cleaned in _SECTOR_ALIASES:
        return _SECTOR_ALIASES[cleaned]

    cleaned_underscore = cleaned.replace(" ", "_")
    if cleaned_underscore in SECTOR_ETF_MAP:
        return cleaned_underscore

    raise ValueError(
        f"Unknown sector name: {sector_name!r}. "
        f"Supported sectors are: {sorted(SECTOR_ETF_MAP.keys())}"
    )


def get_sector_etf(sector_name: str) -> str:
    """
    Return the benchmark sector ETF ticker for a given sector name.

    Examples
    --------
    >>> get_sector_etf("Technology")
    'XLK'
    >>> get_sector_etf("health care")
    'XLV'
    """
    normalized_sector = normalize_sector_name(sector_name)
    return SECTOR_ETF_MAP[normalized_sector]