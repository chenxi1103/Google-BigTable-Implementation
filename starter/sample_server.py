from http.server import HTTPServer, BaseHTTPRequestHandler
import json

class MyHandler(BaseHTTPRequestHandler):
    def _set_response(self, code):
        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_GET(self):
        # example: this is how you get path and command
        print(self.path)
        print(self.command)

        # example: returning an object as JSON
        data = {
            "row": "sample_a",
            "data": [
                {
                    "value": "data_a",
                    "time": "1234"
                }
            ]
        }
        data_json = json.dumps(data)

        self._set_response(200)
        self.wfile.write(data_json.encode("utf8"))

    def do_POST(self):
        # example: reading content from HTTP request
        data = None
        content_length = self.headers['content-length']
    
        if content_length != None:
            content_length = int(content_length)
            data = self.rfile.read(content_length)

            # print the content, just for you to see it =)
            print(data)

        self._set_response(200)

    def do_DELETE(self):
        # example: send just a 200
        self._set_response(200)


if __name__ == "__main__":
    server_address = ("localhost", 8080)
    handler_class = MyHandler
    server_class = HTTPServer

    httpd = HTTPServer(server_address, handler_class)
    print("sample server running...")

    try: 
        httpd.serve_forever()
    except KeyboardInterrupt: pass

    httpd.server_close()

