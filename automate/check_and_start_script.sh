
facilties='all_facilities.pid'
python get_facilities.py

pid_file='mydaemon.pid'
facility_num='facility.pid'

num_required=$(cat $facilties) #Required number of facililities to be downloaded
num_required2=$(($num_required*2))
num_required3=$(($num_required*3))
echo $num_required2


if [ ! -s "$pid_file" ] || ! kill -0 $(cat $pid_file) && [ $(cat $facility_num) -ge "$num_required" -a  $(cat $facility_num) -lt "$num_required2" ] ; then
  	python app.py new current 2 >> /debug.txt 2>&1

elif [ ! -s "$pid_file" ] || ! kill -0 $(cat $pid_file) && [ $(cat $facility_num) -ge "$num_required2" -a  $(cat $facility_num) -lt "$num_required3" ] ; then
  	python app.py new current 1 >> /debug.txt 2>&1

elif [ ! -s "$pid_file" ] || ! kill -0 $(cat $pid_file) && [ $(cat $facility_num) -lt "$num_required" ]  ; then
  	python app.py new current 3 >> /debug.txt 2>&1
fi







