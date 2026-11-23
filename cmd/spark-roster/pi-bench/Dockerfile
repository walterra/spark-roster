FROM node:24-bookworm-slim@sha256:dc26ae92c9fb15cf6b1d63e20f0944b2efe0f115b20e00be48fb23fceb061c54
WORKDIR /opt/bench
COPY --chmod=0644 package.json package-lock.json ./
RUN npm ci --no-audit --no-fund && npm cache clean --force
COPY --chmod=0644 runner.cjs ./
