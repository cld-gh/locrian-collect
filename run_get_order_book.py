#from locrian_collect.schedule_get_order_book import schedule_get_order_book
from locrian_collect.scheduler import scheduler


#schedule_get_order_book()
scheduler(data_to_record='order_book')