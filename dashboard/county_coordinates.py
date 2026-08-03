"""Approximate centre coordinates for the 26 counties of the Republic of
Ireland, used to place bubbles on the map view. Not survey-accurate --
good enough for a visual overview, not for navigation.
"""

COUNTY_COORDINATES: dict[str, tuple[float, float]] = {
    "Carlow": (52.80, -6.90),
    "Cavan": (54.00, -7.40),
    "Clare": (52.85, -8.98),
    "Cork": (51.90, -8.70),
    "Donegal": (54.85, -8.00),
    "Dublin": (53.35, -6.26),
    "Galway": (53.30, -9.00),
    "Kerry": (52.15, -9.55),
    "Kildare": (53.15, -6.90),
    "Kilkenny": (52.65, -7.25),
    "Laois": (53.00, -7.30),
    "Leitrim": (54.10, -8.00),
    "Limerick": (52.50, -8.65),
    "Longford": (53.72, -7.80),
    "Louth": (53.90, -6.50),
    "Mayo": (53.90, -9.40),
    "Meath": (53.60, -6.65),
    "Monaghan": (54.25, -6.97),
    "Offaly": (53.27, -7.50),
    "Roscommon": (53.76, -8.26),
    "Sligo": (54.27, -8.47),
    "Tipperary": (52.60, -7.90),
    "Waterford": (52.20, -7.40),
    "Westmeath": (53.53, -7.50),
    "Wexford": (52.45, -6.50),
    "Wicklow": (52.98, -6.40),
}
