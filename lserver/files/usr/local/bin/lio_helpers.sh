find_lib() {
    #Check the LD_LIBRARY_PATH first
    if [ "$LD_LIBRARY_PATH" != "" ]; then
        LP=$(find $(echo $LD_LIBRARY_PATH | tr -s ":" " ") | grep $1 | sort | head -n 1)
        if [ "${LP}" != "" ]; then
            echo ${LP}
            return
         fi
    fi

    # Check if it's in the default paths
    ldconfig -p | grep $1 | cut -f4 -d\ 
}

get_ld_preload_tcmalloc() {
    TCM=$(find_lib libtcmalloc.so)
    [ "${TCM}" != "" ] && echo "LD_PRELOAD=${TCM}"
}

