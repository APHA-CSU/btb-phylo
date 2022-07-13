#!/usr/bin/bash
set -e

HOSTUSER=$0

REMOTE_SHARE=//fsx-ranch-017.int.sce.network/share/fsx-017
MOUNT_POINT=/mnt/fsx-017

echo " - Attempt mount of $REMOTE_SHARE"

if grep -qs "$REMOTE_SHARE" /proc/mounts; then
	    printf " - Already mounted.  See details:\n $(grep $REMOTE_SHARE /proc/mounts)\n - Exiting.\n"
    else
		mkdir /mnt/fsx-017
		mount -t cifs -o vers=2.1,sec=ntlmsspi,user=$HOSTUSER,uid=$HOSTUSER $REMOTE_SHARE $MOUNT_POINT \
			&& echo " - Successfully mounted at: $MOUNT_POINT"
fi
