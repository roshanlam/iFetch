# iFetch container image.
#
# Two stages, because the runtime layer must not carry a compiler. Wheels are
# built once in `builder` (some transitive dependencies still ship sdists) and
# only the resulting site-packages tree is copied forward.
#
# Two things about this image are load-bearing and easy to get wrong:
#
#   * TMPDIR=/config. pyicloud stores the trusted session under
#     `<tempdir>/pyicloud/<user>`, so with the default /tmp the session dies
#     with the container and every start re-does 2FA. Pointing TMPDIR at a
#     mounted volume is what makes an unattended container possible at all.
#   * USER=ifetch is set as an environment variable as well as a container
#     user. pyicloud derives the session directory name from
#     `getpass.getuser()`, which reads $USER/$LOGNAME first and only then falls
#     back to a /etc/passwd lookup - a lookup that *raises* when the image is
#     run with `--user 1000:1000` and that uid has no passwd entry. Setting it
#     explicitly makes the session path stable no matter how the container is
#     started.

# --------------------------------------------------------------------------
# Stage 1: build wheels
# --------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1

# build-essential is needed only here; it never reaches the runtime stage.
RUN apt-get update \
    && apt-get install --no-install-recommends -y build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Install into an isolated prefix so exactly one directory has to be copied
# forward, with no site-packages archaeology in the final stage.
COPY pyproject.toml ReadMe.md MANIFEST.in ./
COPY ifetch ./ifetch
RUN pip install --prefix=/install .

# --------------------------------------------------------------------------
# Stage 2: runtime
# --------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS runtime

LABEL org.opencontainers.image.title="iFetch" \
      org.opencontainers.image.description="Bulk download, back up, and mirror your iCloud Drive from the command line" \
      org.opencontainers.image.source="https://github.com/roshanlam/iFetch" \
      org.opencontainers.image.licenses="MIT"

# /data  - the mirror destination (your files)
# /config - persistent state: the pyicloud trusted session lives here via TMPDIR
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TMPDIR=/config \
    HOME=/config \
    USER=ifetch \
    LOGNAME=ifetch

# ca-certificates matters for self-hosted Healthchecks or ntfy behind a private
# CA; tini gives the container a real init so `docker stop` interrupts a
# download cleanly instead of killing it mid-write. Nothing else is installed -
# no compiler, no git, no curl.
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates tini \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home-dir /config --shell /usr/sbin/nologin ifetch \
    && mkdir -p /data /config \
    && chown -R ifetch:ifetch /data /config

COPY --from=builder /install /usr/local

WORKDIR /data
USER ifetch

# tini reaps the process properly so `docker stop` interrupts a download
# cleanly and iFetch's own resume state is written on the way out.
ENTRYPOINT ["/usr/bin/tini", "--", "ifetch"]

# No default remote path: which folder to mirror is the one decision that
# cannot be guessed. Override with `command:` in compose or arguments to
# `docker run`.
CMD ["--help"]
