# =============================================================================
# recommendation_engine.py
# =============================================================================
# Reads dikgoboro_bureau_features + dikgoboro_products to recommend the
# best and next best funeral cover product for a client based on their
# bureau profile. Stores product UUIDs (not names) in
# dikgoboro_recommendations.
#
# NERVE SCORE (0 – 7):
#   Signals:
#     effective_credit_score >= 700  → +2
#     effective_credit_score 600-699 → +1
#     effective_credit_score < 600   → +0
#     is_employed                    → +1
#     active_directorships = 1       → +1
#     active_directorships = 2-3     → +2
#     active_directorships = 4+      → +3
#     no adverse SAFPS listing       → +1
#   Max = 7
#
# SCORE → MAX COVER MAPPING:
#   0     → client does not qualify (skipped — best_product_id is NOT NULL)
#   1     → R10 000
#   2     → R20 000
#   3     → R30 000
#   4     → R40 000
#   5–7   → R50 000
#
# BEST PRODUCT:
#   Exact match on (coverage_type, age_range, max_cover) from
#   dikgoboro_products. Walks down cover levels if no product exists
#   for that age band at the target cover.
#
# NEXT BEST PRODUCT:
#   Same coverage_type + age_range, but one cover level below the best
#   product that was found. Null if best is already at R10 000.
#
# COVERAGE TYPE (from dikgoboro_clients.product_interest):
#   'Funeral Insurance'  → 'Single Member'  (default)
#   'Single Member'      → 'Single Member'
#   'Family'             → 'Family'
#   'Single Parent'      → 'Single Parent'
#   'Extended Family'    → 'Extended Family'
#
# AGE BAND:
#   0–17  → '0 - 17'  |  18–64 → '18 - 64'  |  65–74 → '65 - 74'
#   75–84 → '75 - 84' |  85+   → '85+'
#
# OUTPUT → dikgoboro_recommendations:
#   customer_id, best_product_id (uuid FK), next_best_product_id (uuid FK),
#   nerve_score, reason
# =============================================================================

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from postgrest.exceptions import APIError

from app.db.supabase_client import supabase
from app.services.bureau_extractor import (
    extract_bureau_features,
    get_latest_bureau_features,
)

logger = logging.getLogger(__name__)

SORRY_MESSAGE = (
    "Sorry, we could not find a qualifying product that matches "
    "your profile at the moment."
)

# Score → max cover (rand). None = does not qualify.
SCORE_TO_COVER: Dict[int, Optional[int]] = {
    0: None,
    1: 10000,
    2: 20000,
    3: 30000,
    4: 40000,
    5: 50000,
    6: 50000,
    7: 50000,
}

# Ordered highest → lowest for walk-down logic
COVER_LEVELS: List[int] = [50000, 40000, 30000, 20000, 10000]

INTEREST_TO_COVERAGE_TYPE: Dict[str, str] = {
    "funeral insurance": "Single Member",
    "single member":     "Single Member",
    "family":            "Family",
    "single parent":     "Single Parent",
    "extended family":   "Extended Family",
}


# =============================================================================
# Utilities
# =============================================================================

def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(float(str(value).replace(",", "").strip()))
    except Exception:
        return default


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _age_to_band(age: Optional[int]) -> Optional[str]:
    if age is None:
        return None
    if age <= 17:
        return "0 - 17"
    if age <= 64:
        return "18 - 64"
    if age <= 74:
        return "65 - 74"
    if age <= 84:
        return "75 - 84"
    return "85+"


def _resolve_coverage_type(product_interest: Optional[str]) -> str:
    if not product_interest:
        return "Single Member"
    return INTEREST_TO_COVERAGE_TYPE.get(
        product_interest.strip().lower(), "Single Member"
    )


# =============================================================================
# Nerve score
# =============================================================================

def _compute_nerve_score(features: Dict[str, Any]) -> int:
    score = 0

    cs = _safe_int(features.get("effective_credit_score"), 0)
    if features.get("effective_credit_score") is not None:
        if cs >= 700:
            score += 2
        elif cs >= 600:
            score += 1

    if features.get("is_employed"):
        score += 1

    active_dirs = _safe_int(features.get("active_directorships"), 0)
    if active_dirs == 0 and features.get("has_active_directorship"):
        active_dirs = 1
    if active_dirs >= 4:
        score += 3
    elif active_dirs >= 2:
        score += 2
    elif active_dirs == 1:
        score += 1

    if features.get("safps_status", "unknown") != "listed":
        score += 1

    return min(score, 7)


# =============================================================================
# Product fetching
# =============================================================================

def _fetch_product(
    coverage_type: str,
    age_band: str,
    max_cover: int,
) -> Optional[Dict[str, Any]]:
    """Fetch one active product by exact (coverage_type, age_range, max_cover)."""
    try:
        res = (
            supabase()
            .table("dikgoboro_products")
            .select("id, product_code, product_name, coverage_type, max_cover, premium_monthly, age_range, description")
            .eq("coverage_type", coverage_type)
            .eq("age_range", age_band)
            .eq("max_cover", max_cover)
            .eq("active", True)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None
    except Exception as e:
        logger.warning("Product fetch error: %s", e)
        return None


def _find_best_and_next(
    coverage_type: str,
    age_band: str,
    nerve_score: int,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Returns (best_product, next_best_product).

    Best:      highest available cover level the client qualifies for.
    Next best: one cover level below best (same coverage_type + age_band).
    Both may be None if no product exists for the age/type combination.
    """
    max_cover = SCORE_TO_COVER.get(nerve_score)
    if max_cover is None:
        return None, None  # score 0 — does not qualify

    # Walk down from target cover to find best
    start_idx = COVER_LEVELS.index(max_cover) if max_cover in COVER_LEVELS else 0
    best_product: Optional[Dict[str, Any]] = None
    best_cover_idx: Optional[int] = None

    for idx, cover in enumerate(COVER_LEVELS[start_idx:], start=start_idx):
        product = _fetch_product(coverage_type, age_band, cover)
        if product:
            best_product = product
            best_cover_idx = idx
            break

    if best_product is None or best_cover_idx is None:
        return None, None

    # Next best = one step lower in COVER_LEVELS
    next_best_product: Optional[Dict[str, Any]] = None
    next_idx = best_cover_idx + 1
    if next_idx < len(COVER_LEVELS):
        next_best_product = _fetch_product(
            coverage_type, age_band, COVER_LEVELS[next_idx]
        )

    return best_product, next_best_product


# =============================================================================
# Reason builder
# =============================================================================

# Coverage type → human-readable benefit description
_COVERAGE_BENEFIT: Dict[str, str] = {
    "Single Member":   "comprehensive individual coverage",
    "Family":          "comprehensive coverage for immediate family",
    "Single Parent":   "comprehensive coverage for you and your dependants",
    "Extended Family": "comprehensive coverage for your extended family",
}

# Coverage type → who is protected description
_COVERAGE_WHO: Dict[str, str] = {
    "Single Member":   "protecting you as an individual",
    "Family":          "protecting you, your spouse and your children",
    "Single Parent":   "protecting you and your dependants",
    "Extended Family": "protecting you and your broader family circle",
}

# Next best coverage type → alternative suggestion sentence
_ALTERNATIVE_SUGGESTION: Dict[str, str] = {
    "Single Member":   (
        "Alternative: consider upgrading to a Family Plan if you need "
        "coverage for a spouse or children."
    ),
    "Family":          (
        "Alternative: Extended Family Plan if you need broader coverage "
        "for parents, grandparents or siblings."
    ),
    "Single Parent":   (
        "Alternative: Family Plan if your household includes a spouse."
    ),
    "Extended Family": (
        "Alternative: Family Plan for a more focused and affordable option "
        "covering immediate family only."
    ),
}


def _coverage_value_descriptor(cover: int, premium: int) -> str:
    """Return a value descriptor phrase based on cover/premium ratio."""
    ratio = cover / premium if premium > 0 else 0
    if ratio >= 150:
        return "excellent value"
    if ratio >= 100:
        return "great value"
    return "solid value"


def _build_reason(
    best: Dict[str, Any],
    next_best: Optional[Dict[str, Any]],
    features: Dict[str, Any],
    nerve_score: int,
) -> str:
    """
    Build a reason string in the NERVE AI format.

    With next best product (client has options):
      "Based on NERVE AI analysis: {plan} offers {benefit} with {value descriptor}
       at R{premium}/month with R{cover} coverage. This is optimal for {who}.
       Alternative: {next best suggestion}."

    Without next best (client qualifies for one product only):
      "Based on your profile, we recommend {plan} — the ideal plan for {who}.
       It offers R{cover} coverage at R{premium}/month."
    """
    coverage_type = best.get("coverage_type", "Single Member")
    cover         = best.get("max_cover", 0)
    premium       = best.get("premium_monthly", 0)
    plan          = best.get("product_name", "")

    benefit    = _COVERAGE_BENEFIT.get(coverage_type, "comprehensive coverage")
    who        = _COVERAGE_WHO.get(coverage_type, "protecting you and your family")
    value_desc = _coverage_value_descriptor(cover, premium)

    if next_best:
        # Full format — client has both best and next best
        alternative = _ALTERNATIVE_SUGGESTION.get(coverage_type, "")
        return (
            f"Based on NERVE AI analysis: {plan} offers {benefit} "
            f"with {value_desc} at R{premium}/month with R{cover:,} coverage. "
            f"This is optimal for {who}. "
            f"{alternative}"
        ).strip()
    else:
        # Single recommendation — client only qualifies for one product
        return (
            f"Based on your profile, we recommend {plan} — "
            f"the ideal plan for {who}. "
            f"It offers R{cover:,} coverage at R{premium}/month."
        )


# =============================================================================
# DB helpers
# =============================================================================

def _get_client(client_id: str) -> Optional[Dict[str, Any]]:
    try:
        res = (
            supabase()
            .table("dikgoboro_clients")
            .select("id, product_interest, age, date_of_birth")
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
            .select("id")
            .eq("customer_id", client_id)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None
    except Exception:
        return None


def _upsert_recommendation(
    client_id: str,
    best_product_id: str,
    next_best_product_id: Optional[str],
    nerve_score: int,
    reason: str,
    existing: Optional[Dict[str, Any]],
) -> str:
    """Insert or update dikgoboro_recommendations. Returns mode string."""
    row: Dict[str, Any] = {
        "customer_id":          client_id,
        "best_product_id":      best_product_id,
        "next_best_product_id": next_best_product_id,
        "nerve_score":          nerve_score,
        "reason":               reason,
        "updated_at":           _now_iso(),
    }

    if existing:
        (
            supabase()
            .table("dikgoboro_recommendations")
            .update(row)
            .eq("customer_id", client_id)
            .execute()
        )
        return "updated"

    row["created_at"] = _now_iso()
    (
        supabase()
        .table("dikgoboro_recommendations")
        .insert(row)
        .execute()
    )
    return "inserted"


# =============================================================================
# Main entry point
# =============================================================================

def generate_recommendation_for_customer(customer_id: str) -> Dict[str, Any]:
    """
    Full recommendation run for one customer.

      1. Load client from dikgoboro_clients
      2. Derive age band + coverage type
      3. Ensure bureau_features exist (extract if missing)
      4. Compute nerve_score
      5. Find best + next best product UUIDs from dikgoboro_products
      6. Upsert into dikgoboro_recommendations
    """

    # 1. Load client
    client = _get_client(customer_id)
    if not client:
        return {"status": "skipped", "reason": "client_not_found"}

    # 2. Age band
    age = _safe_int(client.get("age"), 0) or None
    if not age and client.get("date_of_birth"):
        try:
            dob   = date.fromisoformat(str(client["date_of_birth"])[:10])
            today = date.today()
            age   = today.year - dob.year - (
                (today.month, today.day) < (dob.month, dob.day)
            )
        except Exception:
            pass

    age_band = _age_to_band(age)
    if not age_band:
        return {"status": "skipped", "reason": "client_age_unknown"}

    coverage_type = _resolve_coverage_type(client.get("product_interest"))

    # 3. Bureau features
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
                extract_result = extract_bureau_features(bp_res.data[0]["id"])
                if extract_result["status"] == "success":
                    features = get_latest_bureau_features(customer_id)
        except Exception as e:
            return {"status": "skipped", "reason": f"bureau_features_load_error: {e}"}

    if not features:
        return {"status": "skipped", "reason": "no_bureau_features_available"}

    if features.get("is_deceased") is True:
        return {"status": "skipped", "reason": "client_is_deceased"}

    # 4. Nerve score
    nerve_score = _compute_nerve_score(features)

    # 5. Find products
    best_product, next_best_product = _find_best_and_next(
        coverage_type, age_band, nerve_score
    )

    # best_product_id is NOT NULL in schema — skip if no product found
    if not best_product:
        return {
            "status":      "skipped",
            "reason":      f"no_qualifying_product (score={nerve_score}, "
                           f"type={coverage_type}, age_band={age_band})",
            "nerve_score": nerve_score,
        }

    best_product_id      = best_product["id"]
    next_best_product_id = next_best_product["id"] if next_best_product else None
    reason = _build_reason(best_product, next_best_product, features, nerve_score)

    # 6. Upsert
    existing = _get_existing_recommendation(customer_id)
    try:
        mode = _upsert_recommendation(
            client_id=customer_id,
            best_product_id=best_product_id,
            next_best_product_id=next_best_product_id,
            nerve_score=nerve_score,
            reason=reason,
            existing=existing,
        )
    except APIError as e:
        return {"status": "error", "reason": f"upsert_failed: {e}"}

    return {
        "status":               "success",
        "mode":                 mode,
        "customer_id":          customer_id,
        "nerve_score":          nerve_score,
        "coverage_type":        coverage_type,
        "age_band":             age_band,
        "best_product_id":      best_product_id,
        "best_product_name":    best_product.get("product_name"),
        "next_best_product_id": next_best_product_id,
        "next_best_product_name": next_best_product.get("product_name") if next_best_product else None,
    }


# =============================================================================
# Batch runner
# =============================================================================

def generate_recommendations_for_all_pending() -> Dict[str, Any]:
    """
    Runs recommendations for all clients in dikgoboro_clients.
    Safe to re-run — existing rows are updated, not duplicated.
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