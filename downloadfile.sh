filepath="$1"
pattern="$2"
if [ -d "$filepath" ];then
find "$filepath"|egrep -i "$pattern"|cpio -o -H tar --quiet|cpio -i --quiet --to-stdout --pattern-file=./test/etc/ssh/ssh_config

#|zcat|cpio -iudm ./test/target
elif [ -f "$filepath" ];then
cat "$filepath"
else
echo "error"
exit 1
fi


