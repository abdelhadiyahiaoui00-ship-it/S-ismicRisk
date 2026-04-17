from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.schemas.policy import PolicyListResponse, PolicyRead, PortfolioSummary
from app.services.policy_service import policy_service

router = APIRouter()


@router.get("", response_model=PolicyListResponse)
async def list_policies(
    page: int = Query(default=1, ge=1),
    size: int = Query(default=50, ge=1, le=200),
    policy_year: int | None = None,
    code_wilaya: str | None = None,
    zone_sismique: str | None = None,
    type_risque: str | None = None,
    commune: str | None = None,
    search: str | None = None,
    db: AsyncSession = Depends(get_db),
) -> PolicyListResponse:
    return await policy_service.list_policies(
        db,
        page=page,
        size=size,
        policy_year=policy_year,
        code_wilaya=code_wilaya,
        zone_sismique=zone_sismique,
        type_risque=type_risque,
        commune=commune,
        search=search,
    )


@router.get("/summary", response_model=PortfolioSummary)
async def get_policy_summary(db: AsyncSession = Depends(get_db)) -> PortfolioSummary:
    return await policy_service.get_summary(db)


@router.get("/{policy_id}", response_model=PolicyRead)
async def get_policy(policy_id: int, db: AsyncSession = Depends(get_db)) -> PolicyRead:
    policy = await policy_service.get_policy(db, policy_id)
    if policy is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Policy not found")
    return PolicyRead.model_validate(policy)
