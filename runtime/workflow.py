"""Versioned, deterministic fixture and access policy for the offline workflow."""

POLICY_VERSION = "synthetic-policy-v1"
# IDs are synthetic and deliberately small; this is not an identity provider.
USERS = {"maya": "Maya"}
WORKSPACES = {"analytics": "approval_required", "admin": "denied"}


def evaluate(action):
    if action["user_id"] not in USERS:
        return "UNKNOWN_USER"
    if action["workspace_id"] not in WORKSPACES:
        return "UNKNOWN_WORKSPACE"
    if WORKSPACES[action["workspace_id"]] == "denied":
        return "DENIED"
    return "APPROVAL_REQUIRED"
