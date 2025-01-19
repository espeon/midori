import http.server
import os
import socketserver

class CORSRequestHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(204)  # No Content
        self.end_headers()

    def do_GET(self):
        # Check if there's a Range header
        if 'Range' in self.headers:
            self.handle_range_request()
        else:
            super().do_GET()

    def handle_range_request(self):
        range_header = self.headers['Range']
        file_size = self.get_file_size()
        if range_header.startswith('bytes='):
            ranges = range_header[6:].split('-')
            start = int(ranges[0])
            end = file_size - 1 if len(ranges) < 2 or ranges[1] == '' else int(ranges[1])

            if start > end or end >= file_size:
                self.send_response(416)  # Range Not Satisfiable
                self.send_header('Content-Range', f'bytes */{file_size}')
                self.end_headers()
                return

            self.send_response(206)  # Partial Content
            self.send_header('Content-Range', f'bytes {start}-{end}/{file_size}')
            self.send_header('Content-Length', str(end - start + 1))
            self.end_headers()

            with open(self.path.strip('/'), 'rb') as f:
                f.seek(start)
                self.wfile.write(f.read(end - start + 1))
        else:
            self.send_response(400)  # Bad Request
            self.end_headers()

    def get_file_size(self):
        try:
            return os.path.getsize(self.path.strip('/'))
        except Exception:
            return 0

PORT = 8000
with socketserver.TCPServer(("", PORT), CORSRequestHandler) as httpd:
    print(f"Serving at port {PORT}")
    httpd.serve_forever()
