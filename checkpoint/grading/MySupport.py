class MySupport:
    @staticmethod
    def url(hostname, port, path):
        portstr = str(port)
        url = f"http://{hostname}:{portstr}{path}"
        return url

