from threading import Thread
import requests

class Query(Thread):
  def __init__(self,url,html_queue):
    Thread.__init__(self)
    self.url = url
    self.html_queue = html_queue

  def run(self):
    self.html_queue.put(requests.get(self.url).text)
    return
