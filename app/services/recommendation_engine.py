# =============================================================================
# recommendation_engine.py
# =============================================================================
# Dikgoboro Funeral Insurance Recommendation Engine
#
# LOGIC:
#   1. Load client from dikgoboro_clients
#   2. Load their bureau features from dikgoboro_bureau_features
#   3. Compute a nerve_score (0-7) from bureau signals
#   4. Determine best coverage_type based on client profile
#   5. Select best product from dikgoboro_products matching:
#        - coverage_type
#        - age_range that fits client's age
#        - active = true
#        - highest max_cover the client qualifies for
#   6. Upsert result into dikgoboro_recommendations
#
# NERVE SCORE SIGNALS:
#   credit_score >= 700     -> +2
#   credit_score 600-699    -> +1
#   is_employed             -> +1
#   active_directorships=1  -> +1
#   active_directorships>=2 -> +2
#   no adverse/safps        -> +1
#   Max = 7
#
# COVERAGE TYPE SELECTION:
#   Married / has partner                              -> Family Plan
#   Single with employment history                     -> Single Parent Plan
#   Has active directorships                           -> Extended Family Plan
#   Default                                            -> Single Member Plan
#
# COVER SELECTION (by nerve_score):
#   Score 0-1 -> R10 000
#   Score 2-3 -> R20 000
#   Score 4-5 -> R30 000
#   Score 6   -> R40 000
#   Score 7   -> R50 000
# =============================================================================

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from postgrest.exceptions import APIError

from app.db.supabase_client import supabase
from app.services.bureau_extractor import (
    extract_bureau_features,
    get_latest_bureau_features,
    _calc_age,
)

SORRY_MESSAGE = (
    "Sorry, I couldn't find any qualifying products that meet your profile at the moment."
)

# Nerve score -> target max_cover
SCORE_TO_COVER: Dict[int, int] = {
    0: 10000,
    1: 10000,
    2: 20000,
    3: 20000,
    4: 30000,
    5: 30000,
    6: 40000,
    7: 50000,
}


# =============================================================================
# Utilities
# =============================================================================

def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(str(value).replace(",", "").strip()))
    except Exception:
        return default


# =============================================================================
# Nerve score computation
# =============================================================================

def _compute_nerve_score(features: Dict[str, Any]) -> int:
    """
    Compute a 0-7 score from bureau feature signals.

    Signals:
      credit_score >= 700     -> +2
      credit_score 600-699    -> +1
      credit_score < 600      -> +0
      is_employed             -> +1
      active_directorships=1  -> +1
      active_directorships>=2 -> +2
      no adverse/safps        -> +1
    """
    score = 0

    effective_score = features.get("effective_credit_score")
    if effective_score is not None:
        cs = _safe_int(effective_score, 0)
        if cs >= 700:
            score += 2
        elif cs >= 600:
            score += 1

    if features.get("is_employed"):
        score += 1

    active_dirs = _safe_int(features.get("active_directorships"), 0)
    if active_dirs == 0 and features.get("has_active_directorship"):
        active_dirs = 1
    if active_dirs >= 2:
        score += 2
    elif active_dirs == 1:
        score += 1

    safps = features.get("safps_status", "unknown")
    if safps != "listed":
        score += 1

    return min(score, 7)


# =============================================================================
# Coverage type selection
# =============================================================================

def _determine_coverage_type(
    client: Dict[str, Any],
    features: Dict[str, Any],
) -> str:
    """
    Determine the most appropriate plan type for the client.

    Rules (checked in order):
      1. Married / has partner          -> Family Plan
      2. Single with employment history -> Single Parent Plan
      3. Has active directorships       -> Extended Family Plan
      4. Default                        -> Single Member Plan
    """
    marital = str(
        features.get("marital_status") or client.get("marital_status") or ""
    ).strip().lower()

    if marital in ("married", "cohabiting", "life partner"):
        return "Family"

    emp_count = _safe_int(features.get("employment_history_count"), 0)
    if emp_count > 0 and marital in ("single", "divorced", "widowed", ""):
        return "Single Parent"

    active_dirs = _safe_int(features.get("active_directorships"), 0)
    if active_dirs > 0 or features.get("has_active_directorship"):
        return "Extended Family"

    return "Single Member"


# =============================================================================
# Age range matching
# =============================================================================

def _age_fits_range(age: int, age_range: str) -> bool:
    """
    Check whether a client age falls within a product age_range string.
    Handles: '18 - 64', '65 - 74', '75 - 84', '85+', '0 - 17'
    """
    age_range = (age_range or "").strip()
    if age_range.endswith("+"):
        try:
            return age >= int(age_range.replace("+", "").strip())
        except Exception:
            return False
    if " - " in age_range:
        try:
            lo, hi = age_range.split(" - ")
            return int(lo) <= age <= int(hi)
        except Exception:
            return False
    return False


# =============================================================================
# Product selection
# =============================================================================

def _select_best_product(
    coverage_type: str,
    client_age: int,
    nerve_score: int,
) -> Optional[Dict[str, Any]]:
    """
    Query dikgoboro_products and return the single best matching product.

    Selection logic:
      1. Filter active products by coverage_type
      2. Filter to rows where age_range fits client_age
      3. Determine target max_cover from nerve_score
      4. Return exact cover match; if not found, fall back to
         highest cover below target; last resort: lowest available
    """
    try:
        res = (
            supabase()
            .table("dikgoboro_products")
            .select("*")
            .eq("coverage_type", coverage_type)
            .eq("active", True)
            .order("max_cover", desc=True)
            .execute()
        )
        products: List[Dict[str, Any]] = res.data or []
    except Exception:
        return None

    # Filter to age-eligible products
    age_eligible = [
        p for p in products
        if p.get("age_range") and _age_fits_range(client_age, p["age_range"])
    ]

    if not age_eligible:
        return None

    target_cover = SCORE_TO_COVER.get(nerve_score, 10000)

    # Exact match
    for p in age_eligible:
        if _safe_int(p.get("max_cover"), 0) == target_cover:
            return p

    # Highest cover at or below target
    below = [p for p in age_eligible if _safe_int(p.get("max_cover"), 0) <= target_cover]
    if below:
        return max(below, key=lambda p: _safe_int(p.get("max_cover"), 0))

    # Last resort: lowest available cover
    return min(age_eligible, key=lambda p: _safe_int(p.get("max_cover"), 0))


# =============================================================================
# Reason builder
# =============================================================================

def _build_reason(
    product: Dict[str, Any],
    nerve_score: int,
    features: Dict[str, Any],
) -> str:
    """Build a plain-language reason string for the recommendation."""
    name      = product.get("product_name", "")
    cover     = _safe_int(product.get("max_cover"), 0)
    premium   = _safe_int(product.get("premium_monthly"), 0)
    age_range = product.get("age_range", "")

    signals = []
    if features.get("effective_credit_score"):
        signals.append("credit profile")
    if features.get("is_employed"):
        signals.append("employment status")
    if _safe_int(features.get("active_directorships"), 0) > 0:
        signals.append("directorship activity")

    intro = (
        f"Based on your {' and '.join(signals)}, we recommend"
        if signals else "We recommend"
    )

    return (
        f"{intro} the {name}. "
        f"This plan provides R{cover:,} funeral cover "
        f"for the {age_range} age group "
        f"at R{premium} per month."
    ).strip()


# =============================================================================
# DB helpers
# =============================================================================

def _get_client(client_id: str) -> Optional[Dict[str, Any]]:
    try:
        res = (
            supabase()
            .table("dikgoboro_clients")
            .select("id, age, date_of_birth, first_name, surname, product_interest")
            .eq("id", client_id)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None
    except Exception:
        return None


def _get_existing_recommendation(client_id: str) -> Optional[Dict[str, Any]]:
    try:
        res = (
            supabase()
            .table("dikgoboro_recommendations")
            .select("id, nerve_score, best_plan")
            .eq("customer_id", client_id)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None
    except Exception:
        return None


# =============================================================================
# Upsert helper
# =============================================================================

def _upsert_recommendation(
    customer_id: str,
    row: Dict[str, Any],
) -> Dict[str, Any]:
    """Insert or update dikgoboro_recommendations for a customer."""
    existing = _get_existing_recommendation(customer_id)
    try:
        if existing:
            (
                supabase()
                .table("dikgoboro_recommendations")
                .update(row)
                .eq("customer_id", customer_id)
                .execute()
            )
            return {"status": "success", "mode": "updated"}
        else:
            row["id"]         = str(uuid.uuid4())
            row["created_at"] = _now_iso()
            (
                supabase()
                .table("dikgoboro_recommendations")
                .insert(row)
                .execute()
            )
            return {"status": "success", "mode": "inserted"}
    except APIError as e:
        return {"status": "error", "reason": f"upsert_failed: {e}"}


# =============================================================================
# Main entry point
# =============================================================================

def generate_recommendation_for_customer(
    customer_id: str,
) -> Dict[str, Any]:
    """
    Full recommendation run for one Dikgoboro customer.

      1. Load client from dikgoboro_clients
      2. Ensure bureau_features exist (extract if missing)
      3. Compute nerve_score from features
      4. Determine coverage_type from profile
      5. Select best product from dikgoboro_products
      6. Upsert into dikgoboro_recommendations
    """

    # 1. Load client
    client = _get_client(customer_id)
    if not client:
        return {"status": "skipped", "reason": "client_not_found"}

    # Resolve age
    client_age = _safe_int(client.get("age"), 0)
    if client_age == 0 and client.get("date_of_birth"):
        client_age = _calc_age(client["date_of_birth"]) or 0

    if client_age == 0:
        return {"status": "skipped", "reason": "client_age_unknown"}

    # 2. Get bureau features — extract if missing
    features = get_latest_bureau_features(customer_id)

    if not features:
        try:
            bp_res = (
                supabase()
                .table("dikgoboro_bureau_profiles")
                .select("id")
                .eq("user_id", customer_id)
                .eq("status", "success")
                .order("verified_at", desc=True)
                .limit(1)
                .execute()
            )
            if bp_res.data:
                bp_id          = bp_res.data[0]["id"]
                extract_result = extract_bureau_features(bp_id)
                if extract_result["status"] != "success":
                    return {
                        "status": "skipped",
                        "reason": f"bureau_extraction_failed: {extract_result.get('reason')}",
                    }
                features = get_latest_bureau_features(customer_id)
        except Exception as e:
            return {"status": "skipped", "reason": f"bureau_features_load_error: {e}"}

    # Allow age-only fallback when no bureau data yet
    features = features or {}

    if features.get("is_deceased") is True:
        return {"status": "skipped", "reason": "client_is_deceased"}

    # 3. Compute nerve score
    nerve_score = _compute_nerve_score(features)

    # 4. Determine coverage type
    coverage_type = _determine_coverage_type(client, features)

    # 5. Select best product
    product = _select_best_product(coverage_type, client_age, nerve_score)

    # Fallback: if nothing found for derived type, try Single Member
    if not product and coverage_type != "Single Member":
        coverage_type = "Single Member"
        product = _select_best_product(coverage_type, client_age, nerve_score)

    if not product:
        sorry_row = {
            "customer_id":     customer_id,
            "nerve_score":     nerve_score,
            "best_plan":       None,
            "reason":          SORRY_MESSAGE,
            "recommendations": {},
            "updated_at":      _now_iso(),
        }
        _upsert_recommendation(customer_id, sorry_row)
        return {"status": "skipped", "reason": "no_qualifying_product"}

    # 6. Build and upsert recommendation
    reason = _build_reason(product, nerve_score, features)

    recommendations_snapshot = {
        "product_code":    product.get("product_code"),
        "product_name":    product.get("product_name"),
        "coverage_type":   product.get("coverage_type"),
        "age_range":       product.get("age_range"),
        "max_cover":       product.get("max_cover"),
        "premium_monthly": product.get("premium_monthly"),
        "nerve_score":     nerve_score,
    }

    row = {
        "customer_id":     customer_id,
        "nerve_score":     nerve_score,
        "best_plan":       product.get("product_name"),
        "reason":          reason,
        "recommendations": recommendations_snapshot,
        "updated_at":      _now_iso(),
    }

    result = _upsert_recommendation(customer_id, row)
    if result.get("status") == "error":
        return result

    return {
        "status":          "success",
        "mode":            result.get("mode"),
        "customer_id":     customer_id,
        "nerve_score":     nerve_score,
        "coverage_type":   coverage_type,
        "best_plan":       product.get("product_name"),
        "max_cover":       product.get("max_cover"),
        "premium_monthly": product.get("premium_monthly"),
        "age_range":       product.get("age_range"),
        "reason":          reason,
    }


# =============================================================================
# Batch runner
# =============================================================================

def generate_recommendations_for_all_pending() -> Dict[str, Any]:
    """
    Run recommendations for every client in dikgoboro_clients.
    """
    try:
        clients = (
            supabase()
            .table("dikgoboro_clients")
            .select("id")
            .execute()
            .data or []
        )
    except Exception as e:
        return {"status": "error", "reason": f"Could not load clients: {e}"}

    results = {
        "inserted": 0,
        "updated":  0,
        "skipped":  0,
        "errors":   0,
        "total":    len(clients),
    }

    for c in clients:
        outcome = generate_recommendation_for_customer(c["id"])
        mode    = outcome.get("mode") or outcome.get("status")

        if mode == "inserted":
            results["inserted"] += 1
        elif mode == "updated":
            results["updated"] += 1
        elif outcome.get("status") == "skipped":
            results["skipped"] += 1
        else:
            results["errors"] += 1

    return {"status": "success", **results}