FROM nginx:alpine
WORKDIR /usr/share/nginx/html
COPY frontend/ui-active/ .
COPY frontend/default.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
