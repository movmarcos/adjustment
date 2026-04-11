"""
Adjustment Engine — Core Business Logic
=========================================
Python-side helpers for computing adjustment deltas, validating inputs,
and orchestrating the adjustment lifecycle.
Used by Streamlit pages and can be imported by stored procedures.
"""

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class AdjustmentType(str, Enum):
    FLATTEN = "FLATTEN"
    SCALE = "SCALE"
    ROLL = "ROLL"


class AdjustmentStatus(str, Enum):
    DRAFT = "DRAFT"
    PENDING_APPROVAL = "PENDING_APPROVAL"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    APPLIED = "APPLIED"
    REVERSED = "REVERSED"


# Valid state transitions
VALID_TRANSITIONS: dict[str, list[str]] = {
    "DRAFT":            ["PENDING_APPROVAL"],
    "PENDING_APPROVAL": ["APPROVED", "REJECTED"],
    "APPROVED":         ["APPLIED"],
    "APPLIED":          ["REVERSED"],
    "REJECTED":         [],
    "REVERSED":         [],
}


@dataclass
class AdjustmentRequest:
    """Represents an adjustment request before it's persisted."""
    adj_type: AdjustmentType
    filter_criteria: dict[str, Any]
    target_date: str
    scale_factor: float = 1.0
    roll_source_date: Optional[str] = None
    business_reason: str = ""
    ticket_reference: Optional[str] = None

    def validate(self) -> list[str]:
        """Validate the adjustment request. Returns list of error messages."""
        errors = []

        if not self.filter_criteria:
            errors.append("At least one filter criterion is required.")

        if not self.target_date:
            errors.append("Target date is required.")

        if not self.business_reason or len(self.business_reason.strip()) < 10:
            errors.append("Business reason must be at least 10 characters.")

        if self.adj_type == AdjustmentType.SCALE:
            if self.scale_factor == 1.0:
                errors.append("Scale factor of 1.0 has no effect.")
            if self.scale_factor == 0:
                errors.append("Use FLATTEN instead of scaling to 0.")

        if self.adj_type == AdjustmentType.ROLL:
            if not self.roll_source_date:
                errors.append("Roll source date is required for ROLL adjustments.")
            if self.roll_source_date == self.target_date:
                errors.append("Roll source date cannot be the same as target date.")

        return errors

    def to_filter_json(self) -> str:
        """Serialize filter criteria to JSON string."""
        return json.dumps(self.filter_criteria, default=str)


def validate_transition(current_status: str, new_status: str) -> tuple[bool, str]:
    """
    Validate a status transition.
    Returns (is_valid, error_message).
    """
    allowed = VALID_TRANSITIONS.get(current_status, [])
    if new_status in allowed:
        return True, ""
    return False, (
        f"Invalid transition: {current_status} → {new_status}. "
        f"Allowed transitions: {allowed}"
    )


def compute_flatten_delta(current_value: float) -> float:
    """Compute the delta to flatten (zero out) a value."""
    return -current_value


def compute_scale_delta(current_value: float, factor: float) -> float:
    """Compute the delta to scale a value by a factor."""
    return current_value * (factor - 1)


def compute_roll_delta(
    current_value: float, source_value: float, scale: float = 1.0
) -> float:
    """Compute the delta to replace current with scaled source value."""
    return (source_value * scale) - current_value


def build_where_clause(filter_criteria: dict[str, Any]) -> str:
    """
    Build a SQL WHERE clause from filter criteria dict.
    Handles both single values and lists (IN clause).
    """
    clauses = []
    for key, value in filter_criteria.items():
        col_name = key.upper()
        if isinstance(value, list):
            quoted = ", ".join([f"'{v}'" for v in value])
            clauses.append(f"{col_name} IN ({quoted})")
        elif value is not None and str(value).strip():
            clauses.append(f"{col_name} = '{value}'")
    return " AND ".join(clauses) if clauses else "1=1"


def format_impact_summary(
    adj_type: str,
    rows_affected: int,
    original_total: float,
    delta_total: float,
    projected_total: float,
) -> str:
    """Format a human-readable impact summary."""
    pct_change = (
        (delta_total / original_total * 100) if original_total != 0 else 0
    )

    lines = [
        f"**{adj_type}** adjustment affecting **{rows_affected:,}** rows",
        f"- Original total: ${original_total:,.2f}",
        f"- Delta: ${delta_total:+,.2f} ({pct_change:+.2f}%)",
        f"- Projected total: ${projected_total:,.2f}",
    ]
    return "\n".join(lines)
