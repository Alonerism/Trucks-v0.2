# Frontend multi-stage build
FROM node:20-alpine AS build
WORKDIR /app

COPY ui-lovable/package.json ui-lovable/package-lock.json* /app/ui-lovable/
WORKDIR /app/ui-lovable
RUN npm ci || npm install

# Build with API base passed by build arg or env
ARG VITE_API_BASE=/api
ENV VITE_API_BASE=${VITE_API_BASE}
COPY ui-lovable /app/ui-lovable
RUN npm run build

FROM nginx:alpine
WORKDIR /usr/share/nginx/html
COPY --from=build /app/ui-lovable/dist .
COPY frontend/default.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
