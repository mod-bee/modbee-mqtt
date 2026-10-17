# The tank rule as a container, to run beside a docker/examples/*-kafka setup.
FROM node:24-bookworm-slim
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev && npm cache clean --force
COPY tank-rule.js ./
USER node
CMD ["node", "tank-rule.js"]
