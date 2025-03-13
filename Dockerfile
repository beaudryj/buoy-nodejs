FROM node:16-alpine as build-stage

WORKDIR /app

# install build dependencies
RUN apk add --no-cache \
    bash \
    build-base \
    git \
    make \
    python3

# install application dependencies
COPY package.json yarn.lock ./
RUN JOBS=max yarn install --non-interactive --frozen-lockfile

# copy in application source
COPY . .

# compile sources
RUN make lib

# prune modules
RUN yarn install --non-interactive --frozen-lockfile --production

# copy built application to runtime image
FROM node:16-alpine
WORKDIR /app

# install curl for health checks
RUN apk add --no-cache curl

COPY --from=build-stage /app/config config
COPY --from=build-stage /app/lib lib
COPY --from=build-stage /app/node_modules node_modules

# setup default env
ENV NODE_ENV production

# app entrypoint
CMD [ "node", "lib/app.js" ]