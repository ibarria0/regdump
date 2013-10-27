from threading import Thread

class Query(Thread):
  def __init__(self,url,html_queue,pool):
    Thread.__init__(self)
    self.url = url
    self.html_queue = html_queue
    self.pool = pool

  def run(self):
    response = self.pool.request('GET', self.url)
    self.html_queue.put(response.data)
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
    response = self.pool.request('GET', url)
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
    url = 'http://201.224.39.199/scripts/nwwisapi.dll/conweb/MESAMENU?TODO=SHOW&ID=%s' % str(self.ficha)
    response = self.pool.request('GET', url)
    return response.data
