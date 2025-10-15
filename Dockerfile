# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Setup a non-root user
RUN groupadd --system --gid 999 nonroot \
 && useradd --system --gid 999 --uid 999 --create-home nonroot

# Install git for GitHub package installation
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Install the project into `/app`
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Ensure installed tools can be executed out of the box
ENV UV_TOOL_BIN_DIR=/usr/local/bin

# Install the package from GitHub
# Use build arg to allow specifying branch/tag/commit
ARG GIT_REF=main
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system "plumberlama @ git+https://github.com/CorrelAid/plumberlama.git@${GIT_REF}"

# Reset the entrypoint, don't invoke `uv`
ENTRYPOINT []

# Use the non-root user to run our application
USER nonroot

# Default command - can be overridden in docker-compose
CMD ["plumberlama", "--help"]
