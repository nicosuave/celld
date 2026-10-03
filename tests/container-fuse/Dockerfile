FROM alpine:3.23
RUN apk add --no-cache fuse-overlayfs
COPY mount.sh /mount.sh
CMD ["sh", "/mount.sh"]
