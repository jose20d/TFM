"""Internal metrics router."""

from os import getenv
from secrets import compare_digest

from fastapi import APIRouter
from fastapi import Header
from fastapi import HTTPException
from fastapi import Query
from pydantic import BaseModel

from web.services import visit_metrics_service

router = APIRouter()


class VisitHitPayload(BaseModel):
    path: str | None = "/"


@router.post("/api/v1/internal/visit-hit")
def api_internal_visit_hit(
    payload: VisitHitPayload,
    referer: str | None = Header(default=None),
    user_agent: str | None = Header(default=None),
    x_forwarded_for: str | None = Header(default=None),
) -> dict:
    if payload.path and (
        payload.path.startswith("/internal-admin")
        or payload.path.startswith("/ctr-geo")
    ):
        return {"ok": True}
    visit_metrics_service.register_visit(
        route_path=payload.path or "/",
        referer=referer,
        user_agent=user_agent,
        source_ip=(x_forwarded_for or "").split(",")[0].strip() or None,
    )
    return {"ok": True}


@router.get("/api/v1/internal/visit-summary")
def api_internal_visit_summary(
    days: int = Query(default=30, ge=1, le=365),
    x_internal_admin_token: str | None = Header(default=None),
) -> dict:
    expected = getenv("INTERNAL_ADMIN_TOKEN", "").strip()
    if not expected:
        raise HTTPException(status_code=503, detail="INTERNAL_ADMIN_TOKEN is not configured.")
    if not x_internal_admin_token or not compare_digest(x_internal_admin_token, expected):
        raise HTTPException(status_code=401, detail="Unauthorized.")
    return visit_metrics_service.get_visit_summary(days=days)
