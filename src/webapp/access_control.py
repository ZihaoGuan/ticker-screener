from __future__ import annotations

from dataclasses import asdict, dataclass, field


RoleName = str
CapabilityName = str


ROLE_VISITOR = "visitor"
ROLE_PREMIUM = "premium"
ROLE_ADMIN = "admin"

CAP_VIEW_RESULTS = "view_results"
CAP_RUN_SCREENERS = "run_screeners"
CAP_MANAGE_EXCLUSIONS = "manage_exclusions"
CAP_SYNC_HISTORY = "sync_history"
CAP_MANAGE_USERS = "manage_users"

ROLE_CAPABILITIES: dict[RoleName, tuple[CapabilityName, ...]] = {
    ROLE_VISITOR: (CAP_VIEW_RESULTS,),
    ROLE_PREMIUM: (CAP_VIEW_RESULTS, CAP_RUN_SCREENERS),
    ROLE_ADMIN: (
        CAP_VIEW_RESULTS,
        CAP_RUN_SCREENERS,
        CAP_MANAGE_EXCLUSIONS,
        CAP_SYNC_HISTORY,
        CAP_MANAGE_USERS,
    ),
}


@dataclass(frozen=True)
class Principal:
    authenticated: bool
    user_id: int | None = None
    email: str | None = None
    role: RoleName = ROLE_VISITOR
    capabilities: tuple[CapabilityName, ...] = field(default_factory=lambda: ROLE_CAPABILITIES[ROLE_VISITOR])
    is_active: bool = False

    def can(self, capability: CapabilityName) -> bool:
        return capability in self.capabilities

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def capabilities_for_role(role: RoleName) -> tuple[CapabilityName, ...]:
    return ROLE_CAPABILITIES.get(role, ROLE_CAPABILITIES[ROLE_VISITOR])


def normalize_role(value: object) -> RoleName:
    role = str(value or "").strip().lower()
    if role in ROLE_CAPABILITIES:
        return role
    return ROLE_VISITOR


def anonymous_principal() -> Principal:
    return Principal(authenticated=False)


def principal_for_user(*, user_id: int, email: str, role: RoleName, is_active: bool) -> Principal:
    normalized_role = normalize_role(role)
    return Principal(
        authenticated=bool(is_active),
        user_id=user_id,
        email=email,
        role=normalized_role,
        capabilities=capabilities_for_role(normalized_role) if is_active else ROLE_CAPABILITIES[ROLE_VISITOR],
        is_active=bool(is_active),
    )
