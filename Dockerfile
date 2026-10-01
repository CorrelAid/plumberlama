# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Setup a non-root user
RUN groupadd --system --gid 999 nonroot \
 && useradd --system --gid 999 --uid 999 --create-home nonroot

# Install the project into `/app`
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Ensure installed tools can be executed out of the box
ENV UV_TOOL_BIN_DIR=/usr/local/bin

# DEFAULT: Install from local source
# This Dockerfile is designed for local development where you have the repo cloned.
# To install a released version from PyPI instead (for deployment without cloning), replace the next two lines with:
#
#   ARG PLUMBERLAMA_VERSION=0.1.0
#   RUN --mount=type=cache,target=/root/.cache/uv \
#       uv pip install --system "plumberlama==${PLUMBERLAMA_VERSION}"
#
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system .

RUN chown -R nonroot:nonroot /app

# Reset the entrypoint, don't invoke `uv`
ENTRYPOINT []

# Use the non-root user to run our application
USER nonroot

CMD ["plumberlama", "etl"]
