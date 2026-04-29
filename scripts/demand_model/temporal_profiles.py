"""
NHTS-derived hourly trip-start distributions by purpose and day type.

Source: National Household Travel Survey (NHTS) 2022 summary tables.
Values are proportions of daily trips starting in each hour (24 elements, sum ≈ 1.0).

Four trip purposes × three day types = 12 profiles.
"""
from __future__ import annotations

import numpy as np

from .config import DayType

# -----------------------------------------------------------------------
# COMMUTE profiles — strong AM/PM peaks on weekdays, flat on weekends
# -----------------------------------------------------------------------

_COMMUTE_WEEKDAY = np.array([
    0.005, 0.003, 0.003, 0.005, 0.010, 0.030,  #  0-5: pre-dawn ramp
    0.075, 0.110, 0.095, 0.060, 0.040, 0.035,  #  6-11: AM peak
    0.035, 0.035, 0.040, 0.055, 0.080, 0.095,  # 12-17: PM peak ramp
    0.070, 0.040, 0.025, 0.018, 0.012, 0.008,  # 18-23: evening taper
], dtype=np.float64)

_COMMUTE_SATURDAY = np.array([
    0.008, 0.005, 0.004, 0.004, 0.005, 0.010,
    0.020, 0.035, 0.050, 0.060, 0.065, 0.065,
    0.060, 0.060, 0.058, 0.055, 0.055, 0.055,
    0.050, 0.048, 0.042, 0.035, 0.025, 0.015,
], dtype=np.float64)

_COMMUTE_SUNDAY = np.array([
    0.008, 0.005, 0.004, 0.004, 0.005, 0.008,
    0.015, 0.025, 0.040, 0.050, 0.058, 0.060,
    0.062, 0.062, 0.060, 0.058, 0.058, 0.058,
    0.055, 0.052, 0.048, 0.040, 0.028, 0.015,
], dtype=np.float64)

# -----------------------------------------------------------------------
# SOCIAL profiles — evening-heavy, especially weekends
# -----------------------------------------------------------------------

_SOCIAL_WEEKDAY = np.array([
    0.008, 0.005, 0.003, 0.003, 0.005, 0.008,
    0.015, 0.020, 0.025, 0.030, 0.035, 0.045,
    0.050, 0.048, 0.045, 0.045, 0.050, 0.065,
    0.080, 0.090, 0.085, 0.070, 0.050, 0.030,
], dtype=np.float64)

_SOCIAL_SATURDAY = np.array([
    0.015, 0.010, 0.008, 0.005, 0.005, 0.005,
    0.008, 0.012, 0.020, 0.030, 0.040, 0.050,
    0.055, 0.055, 0.050, 0.048, 0.050, 0.060,
    0.075, 0.090, 0.095, 0.085, 0.065, 0.035,
], dtype=np.float64)

_SOCIAL_SUNDAY = np.array([
    0.012, 0.008, 0.005, 0.004, 0.004, 0.005,
    0.008, 0.015, 0.025, 0.035, 0.045, 0.055,
    0.060, 0.060, 0.058, 0.058, 0.058, 0.060,
    0.068, 0.075, 0.072, 0.060, 0.045, 0.025,
], dtype=np.float64)

# -----------------------------------------------------------------------
# ERRANDS profiles — midday-heavy, similar weekday/weekend
# -----------------------------------------------------------------------

_ERRANDS_WEEKDAY = np.array([
    0.005, 0.003, 0.003, 0.003, 0.005, 0.010,
    0.020, 0.035, 0.050, 0.065, 0.075, 0.080,
    0.078, 0.075, 0.070, 0.065, 0.060, 0.055,
    0.048, 0.040, 0.032, 0.025, 0.015, 0.008,
], dtype=np.float64)

_ERRANDS_SATURDAY = np.array([
    0.005, 0.003, 0.003, 0.003, 0.005, 0.008,
    0.012, 0.025, 0.045, 0.065, 0.080, 0.085,
    0.085, 0.080, 0.075, 0.068, 0.060, 0.055,
    0.048, 0.042, 0.035, 0.028, 0.018, 0.010,
], dtype=np.float64)

_ERRANDS_SUNDAY = np.array([
    0.005, 0.003, 0.003, 0.003, 0.005, 0.008,
    0.010, 0.018, 0.030, 0.050, 0.068, 0.078,
    0.082, 0.082, 0.080, 0.075, 0.068, 0.062,
    0.055, 0.048, 0.040, 0.032, 0.022, 0.012,
], dtype=np.float64)

# -----------------------------------------------------------------------
# TOURISM profiles — late morning start, heavy evening entertainment
# -----------------------------------------------------------------------

_TOURISM_WEEKDAY = np.array([
    0.005, 0.003, 0.003, 0.003, 0.005, 0.008,
    0.012, 0.018, 0.025, 0.035, 0.045, 0.055,
    0.058, 0.058, 0.055, 0.052, 0.055, 0.065,
    0.078, 0.088, 0.085, 0.072, 0.055, 0.030,
], dtype=np.float64)

_TOURISM_SATURDAY = np.array([
    0.012, 0.008, 0.005, 0.004, 0.004, 0.005,
    0.008, 0.012, 0.020, 0.032, 0.042, 0.052,
    0.058, 0.058, 0.055, 0.050, 0.052, 0.062,
    0.078, 0.092, 0.098, 0.088, 0.068, 0.038,
], dtype=np.float64)

_TOURISM_SUNDAY = np.array([
    0.010, 0.006, 0.004, 0.004, 0.004, 0.005,
    0.008, 0.015, 0.025, 0.038, 0.050, 0.060,
    0.065, 0.065, 0.062, 0.060, 0.058, 0.060,
    0.068, 0.075, 0.072, 0.060, 0.045, 0.025,
], dtype=np.float64)

# -----------------------------------------------------------------------
# Profile registry
# -----------------------------------------------------------------------

_PROFILES: dict[tuple[str, DayType], np.ndarray] = {
    ("commute", "weekday"): _COMMUTE_WEEKDAY,
    ("commute", "saturday"): _COMMUTE_SATURDAY,
    ("commute", "sunday"): _COMMUTE_SUNDAY,
    ("social", "weekday"): _SOCIAL_WEEKDAY,
    ("social", "saturday"): _SOCIAL_SATURDAY,
    ("social", "sunday"): _SOCIAL_SUNDAY,
    ("errands", "weekday"): _ERRANDS_WEEKDAY,
    ("errands", "saturday"): _ERRANDS_SATURDAY,
    ("errands", "sunday"): _ERRANDS_SUNDAY,
    ("tourism", "weekday"): _TOURISM_WEEKDAY,
    ("tourism", "saturday"): _TOURISM_SATURDAY,
    ("tourism", "sunday"): _TOURISM_SUNDAY,
}

# Normalize all profiles to sum to 1.0
for key in _PROFILES:
    _PROFILES[key] = _PROFILES[key] / _PROFILES[key].sum()


def get_temporal_profile(
    purpose: str,
    day_type: DayType,
    peak_sharpness: float = 1.0,
) -> np.ndarray:
    """
    Return a 24-element array of hourly trip fractions for the given purpose
    and day type, with peak_sharpness applied.

    peak_sharpness > 1 = sharper peaks (harder for fleet)
    peak_sharpness < 1 = flatter profile (easier for fleet)
    peak_sharpness = 1 = raw NHTS profile
    """
    key = (purpose, day_type)
    if key not in _PROFILES:
        raise ValueError(f"Unknown profile: purpose={purpose}, day_type={day_type}")

    profile = _PROFILES[key].copy()

    if peak_sharpness != 1.0:
        profile = profile ** peak_sharpness
        profile /= profile.sum()

    return profile


def get_all_purposes() -> list[str]:
    """Return list of all trip purpose names."""
    return ["commute", "social", "errands", "tourism"]
