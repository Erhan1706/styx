FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    VENV_PATH=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH="/app"

RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ \
    && python -m venv /opt/venv \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd styx && useradd -l -m -d /usr/local/styx -g styx styx

ENV PYTHONPATH="/usr/local/styx"
ENV UV_LINK_MODE=copy
WORKDIR /usr/local/styx

USER styx

COPY --chown=styx:styx coordinator/pyproject.toml coordinator/uv.lock ./

RUN id styx

# When true, install the optional 'autoscale' extra (chronos, pandas, torch).
# Kept out of the default build so non-autoscale runs stay lightweight.
ARG install_autoscale=false

# Stage 1: Install all external deps (cached)
RUN --mount=type=cache,target=/home/styx/.cache/uv \
    if [ "$install_autoscale" = "true" ]; then \
        uv sync --frozen --no-install-package styx --extra autoscale; \
    else \
        uv sync --frozen --no-install-package styx; \
    fi

COPY --chown=styx:styx styx-package /usr/local/styx-package/

# Stage 2: Copy local package and finish
RUN --mount=type=cache,target=/home/styx/.cache/uv \
    if [ "$install_autoscale" = "true" ]; then \
        uv sync --frozen --extra autoscale; \
    else \
        uv sync --frozen; \
    fi

COPY --chown=styx:styx coordinator coordinator
COPY --chown=styx:styx models models
COPY --chown=styx:styx coordinator/start-coordinator.sh /usr/local/bin/

RUN chmod a+x /usr/local/bin/start-coordinator.sh

EXPOSE 8888

ARG epoch_size=100
ARG enable_compression=true
ARG use_composite_keys=true

ENV SEQUENCE_MAX_SIZE=$epoch_size \
    ENABLE_COMPRESSION=$enable_compression \
    USE_COMPOSITE_KEYS=$use_composite_keys \
    USE_FALLBACK_CACHE=$use_fallback_cache

CMD ["/usr/local/bin/start-coordinator.sh"]