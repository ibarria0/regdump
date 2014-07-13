from threading import Thread
from urllib3 import make_headers

class Query(Thread):
  def __init__(self,url,html_queue,pool):
    Thread.__init__(self)
    self.url = url
    self.html_queue = html_queue
    self.pool = pool

  def run(self):
    response = self.pool.request('GET', self.url)
    self.html_queue.put(response.data.decode('ISO-8859-1','ignore'))
    return

class SociedadQuery(Thread):
  def __init__(self,sociedad,sociedades_queue,pool):
    Thread.__init__(self)
    self.sociedad = sociedad
    self.sociedades_queue = sociedades_queue
    self.pool = pool

  def run(self):
    self.get_ficha_html()
    self.sociedades_queue.put(self.sociedad)
    return

  def get_ficha_html(self):
    url = '/scripts/nwwisapi.dll/conweb/MESAMENU?TODO=SHOW&ID=%s' % str(self.sociedad.ficha)
    response = self.pool.request('GET', url, headers=make_headers(user_agent="Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36"))
    self.sociedad.html = response.data
    return True

class FichaQuery(Thread):
  def __init__(self,ficha,html_queue,pool):
    Thread.__init__(self)
    self.ficha = ficha
    self.html_queue = html_queue
    self.pool = pool

  def run(self):
    html = self.get_ficha_html()
    self.html_queue.put(html)
    return

  def get_ficha_html(self):
    url = '/scripts/nwwisapi.dll/conweb/MESAMENU?TODO=SHOW&ID=%s' % str(self.ficha)
    response = self.pool.request('GET', url, headers=make_headers(user_agent="Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36"))
    return response.data.decode('ISO-8859-1','ignore')
