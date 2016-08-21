#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ceph import Ceph

from functools import wraps
from time import time


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print 'func:%r args:[%r, %r] took: %2.4f sec' % (f.__name__, args, kw, te - ts)
        return result

    return wrap


def check_prerequisites(image, snapshot):
    c = Ceph()
    if c.is_image_exists(image_name=image):
        if snapshot and c.is_snapshot_exists(image, snapshot):
            return c
    raise ValueError("Not found image or snapshot")


@timing
def export_image_native(image, out_file, snapshot=None):
    with check_prerequisites(image, snapshot) as c:
        return c.create_dump_native(image, snapshot, out_file, bs=4*2**20)


@timing
def export_image_popen(image, out_dir, snapshot=None):
    with check_prerequisites(image, snapshot) as c:
        c.create_dump(image, snapshot, out_dir)

if __name__ == '__main__':
    import logging
    logger = logging.getLogger(__file__)
    import argparse, os
    parser = argparse.ArgumentParser()
    parser.add_argument('image_spec', nargs='?', help="Image spec in format pool/image@snapshot")
    parser.add_argument('-o', '--output', default=os.getcwd())
    parser.add_argument('-v', dest='verbose', action='store_true')
    args = parser.parse_args()

    def parse_spec(spec):
        import collections
        _spec = collections.namedtuple('spec', ['pool', 'image', 'snap'])
        pool = image = snap = None
        if '/' not in spec:
            # no pool -> failed to default
            pool = 'rbd'
            image, snap = spec.split('@', 1)
        elif '@' not in spec:
            logger.critical("No snapshot specified!")
            exit(1)
        else:
            pool = spec.split('/', 1)[0]
            image, snap = spec.split('@', 1)
        return _spec(pool=pool, image=image, snap=snap)

    spec = parse_spec(args.image_spec)
    print spec
    try:
        print "popen func."
        export_image_popen(spec.image, args.output, spec.snap)
        os.unlink(os.path.join(args.output, spec.snap))

        print "native func."
        export_image_native(spec.image, os.path.join(args.output, spec.snap), snapshot=spec.snap)
        os.unlink(os.path.join(args.output, spec.snap))
    except KeyboardInterrupt:
        logger.critical("interrupted!")
        if os.path.exists(os.path.join(args.output, spec.snap)):
            os.unlink(os.path.join(args.output, spec.snap))
        exit(2)

