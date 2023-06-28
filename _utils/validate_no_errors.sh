if [ $(grep "error" out/*.err | wc -l) = 0 ]
then
	# no true errors found
	exit 0;
fi
# errors found
echo "The following errors have been found. Failing check"
grep -i "error" out/*.err
exit 1