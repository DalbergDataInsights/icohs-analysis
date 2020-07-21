from crontab import CronTab
import datetime

from crontab import CronTab
 
my_cron = CronTab(user='dhis_api')
job = my_cron.new(command='python  dhis_api.py')
job.month.every(1)

