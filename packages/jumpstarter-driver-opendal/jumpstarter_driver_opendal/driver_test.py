from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from random import randbytes
from tempfile import TemporaryDirectory
from threading import Thread

import pytest
from opendal import Operator

from .common import PresignedRequest
from .driver import MockStorageMux, Opendal
from jumpstarter.common.utils import serve


def test_drivers_opendal(tmp_path):
    with serve(Opendal(scheme="fs", kwargs={"root": str(tmp_path)})) as client:
        assert not client.capability().presign

        client.create_dir("test_dir/")
        client.create_dir("demo_dir/nest_dir/")

        assert client.exists("test_dir/")
        assert client.exists("demo_dir/nest_dir/")

        assert client.stat("test_dir/").mode.is_dir

        assert sorted(client.list("/")) == ["/", "demo_dir/", "test_dir/"]
        assert sorted(client.scan("/")) == ["/", "demo_dir/", "demo_dir/nest_dir/", "test_dir/"]

        test_file = client.open("test_dir/test_file", "wb")
        assert not test_file.closed
        assert not test_file.readable()
        assert not test_file.seekable()
        assert test_file.writable()

        (tmp_path / "src").write_text("hello")
        test_file.write(tmp_path / "src")

        test_file.close()
        assert test_file.closed

        test_file = client.open("test_dir/test_file", "rb")
        assert not test_file.closed
        assert test_file.readable()
        assert test_file.seekable()
        assert not test_file.writable()

        assert test_file.tell() == 0
        assert test_file.seek(2) == 2

        assert client.hash("test_dir/test_file", "md5") == "5d41402abc4b2a76b9719d911017c592"
        assert (
            client.hash("test_dir/test_file", "sha256")
            == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        )

        test_file.read(tmp_path / "dst")
        assert (tmp_path / "dst").read_text() == "llo"

        assert client.stat("dst").content_length == 3

        test_file.close()
        assert test_file.closed

        client.copy("test_dir/test_file", "test_dir/copy_file")
        client.rename("test_dir/copy_file", "test_dir/rename_file")
        assert not client.exists("test_dir/copy_file")
        assert client.exists("test_dir/rename_file")

        client.delete("test_dir/rename_file")
        assert not client.exists("test_dir/rename_file")

        client.remove_all("test_dir/")
        assert not client.exists("test_dir/")

    with serve(Opendal(scheme="http", kwargs={"endpoint": "http://invalid.invalid"})) as client:
        assert client.presign_read("test", 100) == PresignedRequest(
            url="http://invalid.invalid/test", method="GET", headers={}
        )
        assert client.presign_stat("test", 100) == PresignedRequest(
            url="http://invalid.invalid/test", method="HEAD", headers={}
        )


def test_drivers_mock_storage_mux_fs(monkeypatch: pytest.MonkeyPatch):
    with serve(MockStorageMux()) as client:
        with TemporaryDirectory() as tempdir:
            # original file on the client to be pushed to the exporter
            original = Path(tempdir) / "original"
            # new file read back from the exporter to the client
            readback = Path(tempdir) / "readback"

            # test accessing files with absolute path

            # fill the original file with random bytes
            original.write_bytes(randbytes(1024 * 1024 * 10))
            # write the file to the storage on the exporter
            client.write_local_file(str(original))
            # read the storage on the exporter to a local file
            client.read_local_file(str(readback))
            # ensure the contents are equal
            assert original.read_bytes() == readback.read_bytes()

            # test accessing files with relative path
            with monkeypatch.context() as m:
                m.chdir(tempdir)

                original.write_bytes(randbytes(1024 * 1024 * 1))
                client.write_local_file("original")
                client.read_local_file("readback")
                assert original.read_bytes() == readback.read_bytes()

                original.write_bytes(randbytes(1024 * 1024 * 1))
                client.write_local_file("./original")
                client.read_local_file("./readback")
                assert original.read_bytes() == readback.read_bytes()


def test_drivers_mock_storage_mux_http():
    # dummy HTTP server returning static test content
    class StaticHandler(BaseHTTPRequestHandler):
        def do_HEAD(self):
            self.send_response(200)
            self.send_header("content-length", 11 * 1000)
            self.end_headers()

        def do_GET(self):
            self.send_response(200)
            self.send_header("content-length", 11 * 1000)
            self.end_headers()
            self.wfile.write(b"testcontent" * 1000)

    with serve(MockStorageMux()) as client:
        # start the HTTP server
        server = HTTPServer(("127.0.0.1", 8080), StaticHandler)
        server_thread = Thread(target=server.serve_forever)
        server_thread.daemon = True
        server_thread.start()

        # write a remote file from the http server to the exporter
        fs = Operator("http", endpoint="http://127.0.0.1:8080")
        client.write_file(fs, "test")

        server.shutdown()
