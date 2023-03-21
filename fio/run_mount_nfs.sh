#!/bin/bash


BLOCK_SIZES=("524288")
IO_DEPTHS=("1" "100")
NFS_VERSS=("3" "4")
NCONNECTS=("1" "16")
NFS_URL=192.168.20.20:/data
MNT_DIR=/mnt/fb200
CP1_SRC=$MNT_DIR/write.0.0
CP1_DST=/dev/null
CP2_SRC=$MNT_DIR/write.0.0
CP2_DST=$MNT_DIR/read.0.0
SLEEP_TIME=10

# Iterate through block sizes and IO depths

for NFS_VERS in "${NFS_VERSS[@]}"
do
    for NCONNECT in "${NCONNECTS[@]}"
    do
        output_file="outputs/fio_nfsver_${NFS_VERS}_nc_${NCONNECT}"

        # if nconnect is gt 1 and nfs vers is 4
        # we break from the loop, FB doesn't support nconnnect with nfsv4
        if [ $NCONNECT -gt 1 ] && [ $NFS_VERS -eq 4 ]
        then
            break
        fi

        # unmount nfs, clear any previous mounts.
        umount $MNT_DIR 2&>1 > /dev/null
        
        # mount nfs if nconnect is 16 and nfs vers is 3
        if [ $NCONNECT -gt 1 ] && [ $NFS_VERS -eq 3 ]
        then
            echo "mounting nfs:" mount -t nfs -o vers=3,nconnect=$NCONNECT $NFS_URL $MNT_DIR 
            mount -t nfs -o vers=3,nconnect=$NCONNECT $NFS_URL $MNT_DIR
        else
            echo "mount nfs: " mount -t nfs -o vers=$NFS_VERS $NFS_URL $MNT_DIR
            mount -t nfs -o vers=$NFS_VERS $NFS_URL $MNT_DIR
        fi


        # check mount return code and break if not 0
        if [ $? -ne 0 ]
        then
            echo "mount failed, skipping"
            break
        fi


        # check the mount with fbcheck
        ../fbcp -checkmount $MNT_DIR 2&>1 >> ${output_file}.notes

        # log the mount settings
        mount | grep $MNT_DIR >> ${output_file}.notes

        echo "Running CP1: time /usr/bin/cp -r $CP1_SRC $CP1_DST" | tee ${output_file}_cp1
        du -hs $CP1_SRC >> ${output_file}_cp1
        { time /usr/bin/cp -r $CP1_SRC $CP1_DST ; } 2>>  ${output_file}_cp1
        echo Sleeping for $SLEEP_TIME seconds `sleep $SLEEP_TIME`

        echo "Running CP2: time /usr/bin/cp -r $CP2_SRC $CP2_DST" | tee ${output_file}_cp2
        du -hs $CP2_SRC >> ${output_file}_cp2
        { time /usr/bin/cp -r $CP2_SRC $CP2_DST ; } 2>>  ${output_file}_cp2
        echo Sleeping for $SLEEP_TIME seconds `sleep $SLEEP_TIME`


        for block_size in "${BLOCK_SIZES[@]}"
        do
            for io_depth in "${IO_DEPTHS[@]}"
            do
                # Set up output file name
                output_file_fio=${output_file}_bs_${block_size}_iod_${io_depth}

                
                echo
                echo Running Write Test `date`
                # Set up output file name
                fio --bs=$block_size --iodepth=$io_depth libnfs_write.fio  --output=${output_file_fio}_write
                echo
                echo Write Test Complete `date`
		        echo
                echo Running Read Test `date`
                echo
                # Set up output file name
                fio --bs=$block_size --iodepth=$io_depth libnfs_read.fio  --output=${output_file_fio}_read
                echo
                echo Read Test Complete `date`
                echo

	
                echo Sleeping for $SLEEP_TIME seconds `sleep $SLEEP_TIME`
            done
        done

        # unmount nfs
        umount $MNT_DIR
    done
done

