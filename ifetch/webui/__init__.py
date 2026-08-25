"""A local web UI for iFetch: an HTTP server, a job runner, nothing persistent.

Everything here is a front end onto commands that already exist. It stores no
state on disk, it adds no dependency beyond the standard library, and it only
ever listens on the loopback interface unless it is explicitly told otherwise
and says so when it is.

``server`` holds the HTTP layer and the access control that matters, because
this process carries an authenticated iCloud session. ``jobs`` holds the
threading, because a download that takes an hour cannot be an HTTP request.
"""

from .jobs import Job, JobCancelled, JobConflict, JobRunner, Progress, ProgressPlugin
from .server import ApiError, AuthSession, WebUIApp, WebUIServer, create_server

__all__ = [
    "ApiError",
    "AuthSession",
    "Job",
    "JobCancelled",
    "JobConflict",
    "JobRunner",
    "Progress",
    "ProgressPlugin",
    "WebUIApp",
    "WebUIServer",
    "create_server",
]
