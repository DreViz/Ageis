"""Private fault boundary for local synthetic evaluation; never use with real services."""
import httpx
from urllib.parse import urlparse


PROFILES = ("normal", "before-send", "timeout-before-commit", "timeout-after-commit", "unresolved", "stale-lookup")
HOSTS = {"mock-identity", "mock-notice", "localhost", "127.0.0.1", "::1"}


class FaultBoundary:
    def __init__(self, client, workflow, scope, profile):
        if workflow not in ("access", "notice") or profile not in PROFILES:
            raise ValueError("Unsupported synthetic fault profile")
        if urlparse(str(client.base_url)).hostname not in HOSTS:
            raise ValueError("Fault injection requires a local synthetic service")
        self.client, self.workflow, self.scope, self.profile = client, workflow, scope, profile
        self.send_path = "/grants" if workflow == "access" else "/notices"
        self.lookup_path = "/operations/by-key" if workflow == "access" else "/notices/status"
        self.sent = False
        self.stale = False
        self.injections = []
        if workflow == "notice":
            scenario = {"timeout-before-commit": "before_commit", "timeout-after-commit": "after_commit",
                        "unresolved": "after_commit", "stale-lookup": "after_commit"}.get(profile, "normal")
            response = client.put(f"/test/scenario/{scope}", params={"scenario": scenario})
            response.raise_for_status()

    def post(self, path, **kwargs):
        if path != self.send_path:
            raise ValueError("Unexpected synthetic send path")
        first = not self.sent
        self.sent = True
        if first and self.profile == "before-send":
            self.injections.append("before_send")
            raise httpx.ConnectTimeout("Injected before send", request=self.client.build_request("POST", path))
        if first and self.profile in ("timeout-before-commit", "timeout-after-commit", "unresolved", "stale-lookup"):
            point = "before_commit" if self.profile == "timeout-before-commit" else "after_commit"
            self.injections.append(point)
            if self.workflow == "access":
                kwargs["headers"] = {**kwargs.get("headers", {}), "X-Test-Fault": point}
            kwargs["timeout"] = .2
        return self.client.post(path, **kwargs)

    def get(self, path, **kwargs):
        if path != self.lookup_path:
            raise ValueError("Unexpected synthetic lookup path")
        if self.profile == "unresolved":
            self.injections.append("lookup_unavailable")
            return httpx.Response(503, request=self.client.build_request("GET", path))
        if self.profile == "stale-lookup" and not self.stale:
            self.stale = True
            self.injections.append("stale_lookup_absent")
            return httpx.Response(404, request=self.client.build_request("GET", path))
        return self.client.get(path, **kwargs)
