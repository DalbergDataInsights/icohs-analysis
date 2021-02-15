
cd /home/nlubalo/Desktop/dhis2/dhis2-api/
. /home/nlubalo/.local/share/virtualenvs/dhis2-api-2H5ekP__/bin/activate
facilties='/home/nlubalo/Desktop/dhis2/dhis2-api/all_facilities.pid'
python get_facilities.py

pid_file='/tmp/mydaemon.pid'
facility_num='/home/nlubalo/Desktop/dhis2/dhis2-api/facility.pid'
num_required=$(cat $facilties) #Required number of facililities to be downloaded
num_required2=$(($num_required*2))
num_required3=$(($num_required*3))
echo $num_required2


if [ ! -s "$pid_file" ] || ! kill -0 $(cat $pid_file) && [ $(cat $facility_num) -ge "$num_required" -a  $(cat $facility_num) -lt "$num_required2" ] ; then
	cd /home/nlubalo/Desktop/dhis2/dhis2-api/
	. /home/nlubalo/.local/share/virtualenvs/dhis2-api-2H5ekP__/bin/activate
  	python app.py new current 2 >> /home/nlubalo/Desktop/dhis2/dhis2-api/debug.txt 2>&1

elif [ ! -s "$pid_file" ] || ! kill -0 $(cat $pid_file) && [ $(cat $facility_num) -ge "$num_required2" -a  $(cat $facility_num) -lt "$num_required3" ] ; then
	
       cd /home/nlubalo/Desktop/dhis2/dhis2-api/
	. /home/nlubalo/.local/share/virtualenvs/dhis2-api-2H5ekP__/bin/activate
  	python app.py new current 1 >> /home/nlubalo/Desktop/dhis2/dhis2-api/debug.txt 2>&1

elif [ ! -s "$pid_file" ] || ! kill -0 $(cat $pid_file) && [ $(cat $facility_num) -lt "$num_required" ]  ; then
    
    cd /home/nlubalo/Desktop/dhis2/dhis2-api/
	. /home/nlubalo/.local/share/virtualenvs/dhis2-api-2H5ekP__/bin/activate
  	python app.py new current 3 >> /home/nlubalo/Desktop/dhis2/dhis2-api/debug.txt 2>&1
fi







