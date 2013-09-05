#!/bin/sh

export common_tarball=dataserver.tar.gz
export DS_BASE_DIR=../
export COMMON_DIR="conf bin storage include lib log src"
function pack_common()
{
	if [ -f ${common_tarball} ]
	then
		rm ${common_tarball}
	fi

	echo "packing common files"
	
	CURRENT_PWD=`pwd`
	cd $DS_BASE_DIR
	tar czf $CURRENT_PWD/${common_tarball} ${COMMON_DIR}
	cd -
}
