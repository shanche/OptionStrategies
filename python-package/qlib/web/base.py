import dryscrape
# import logging


class BaseScraper(object):

    def __init__(self):
        self._session = dryscrape.Session()

    def read_url(self, url):
        """
        In case that the page is generated by embedded JavaScripts,
        use dryscrape.Session to preprocess the page.
        """
        self._session.visit(url)
        page = self._session.body()
        return page