FROM nginx:1.28-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY index.html styles.css /usr/share/nginx/html/
COPY src /usr/share/nginx/html/src
COPY assets /usr/share/nginx/html/assets
COPY config.docker.js /usr/share/nginx/html/config.js
