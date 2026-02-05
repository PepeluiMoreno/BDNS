#!/bin/sh

# Reemplazar placeholder con variable de entorno
find /usr/share/nginx/html -type f \( -name "*.js" -o -name "*.html" \) -exec \
    sed -i "s|__VITE_GRAPHQL_URL__|${VITE_GRAPHQL_URL:-http://localhost:8000/graphql}|g" {} +

exec "$@"