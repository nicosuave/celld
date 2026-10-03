#!/bin/sh
set -eu

mkdir -p /lower /upper /work /mnt/data
printf 'from lower layer\n' > /lower/input
fuse-overlayfs -f -o lowerdir=/lower,upperdir=/upper,workdir=/work /mnt/data &
fuse_pid=$!
trap 'umount /mnt/data 2>/dev/null || true; kill "$fuse_pid" 2>/dev/null || true' EXIT

attempt=0
until mountpoint -q /mnt/data; do
    if ! kill -0 "$fuse_pid" 2>/dev/null; then
        echo 'FUSE mount failed' >&2
        exit 1
    fi
    attempt=$((attempt + 1))
    test "$attempt" -lt 30 || exit 1
    sleep 0.1
done

test "$(cat /mnt/data/input)" = 'from lower layer'
printf 'written through FUSE\n' > /mnt/data/output
test "$(cat /mnt/data/output)" = 'written through FUSE'
umount /mnt/data
wait "$fuse_pid"
test "$(cat /upper/output)" = 'written through FUSE'
echo 'FUSE read, write, and unmount passed'
