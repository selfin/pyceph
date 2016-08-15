#!/usr/bin/env python
# -*- coding: utf-8 -*-
from threading import Thread
from collections import deque
# It's faster than straightforward implementation you may found in `Ceph:create_dump_native`

cache_size = 6  # 32mb
q = deque(maxlen=cache_size)


class Reader(Thread):
    def __init__(self, image):
        super(Reader, self).__init__()
        self.image = image

    def read(self):
        global q
        CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL = 0x8
        CEPH_OSD_OP_FLAG_FADVISE_NOCACHE = 0x40
        fadvice_flags = CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL | CEPH_OSD_OP_FLAG_FADVISE_NOCACHE
        bs = 10 * 1024**2
        total = self.image.stat()['size']
        cur = 0
        while cur < total:
            try:
                data = self.image.read(cur, bs, fadvice_flags)
            except rbd.InvalidArgument:
                bs = total % bs
                data = self.image.read(cur, bs, fadvice_flags)
            q.append(data)
            cur += bs
        q.append("DONE")
        self.image.close()
        return

    def run(self):
        self.read()


class Writer(Thread):
    def __init__(self, fn):
        super(Writer, self).__init__()
        self.fd = open(fn, 'wb')
        self.stopped = False

    def run(self):
        global q
        i = 0
        while True and not self.stopped:
            data = None
            try:
                data = q.popleft()
            except IndexError:
                pass

            if data == "DONE":
                break

            if data and data != "DONE":
                self.fd.write(data)
            if i % 10 == 0:
                self.fd.flush()
            i += 1

        print "closing..."
        self.fd.close()

    def stop(self):
        self.stopped = True


from .ceph import Ceph

c = Ceph()
import rbd
image = rbd.Image(c.ioctx, "vm-120-disk-1", "nginxsel-vm-120-disk-1-20160525-235912")
#image = rbd.Image(c.ioctx, "test_diff2", "vm1-test_diff2-20160525-165714")

r = Reader(image)
w = Writer("/home/l/diff")

w.daemon = True
r.daemon = True

w.start()
r.start()

w.join()
r.join()


