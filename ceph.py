#!/usr/bin/python
# -*- coding: utf-8 -*-
# Manage ceph images and snapshots
from __future__ import division

__author__ = 'selfin'
import rbd
from subprocess import check_output, CalledProcessError, STDOUT
from rados import Rados
from rados import ObjectNotFound
import logging
import os

logger = logging.getLogger(__name__)
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.DEBUG)


class PoolNotFound(Exception):
    pass


class RBDFeatures(object):
    RBD_FEATURE_LAYERING = 1 << 0
    RBD_FEATURE_STRIPINGV2 = 1 << 1
    RBD_FEATURE_EXCLUSIVE_LOCK = 1 << 2
    RBD_FEATURE_OBJECT_MAP = 1 << 3
    RBD_FEATURE_FAST_DIFF = 1 << 4
    RBD_FEATURE_DEEP_FLATTEN = 1 << 5
    RBD_FEATURE_JOURNALING = 1 << 6

    @classmethod
    def default_features(cls):
        return cls.RBD_FEATURE_LAYERING | cls.RBD_FEATURE_OBJECT_MAP | cls.RBD_FEATURE_EXCLUSIVE_LOCK

    @staticmethod
    def parse_features(features):
        bytes_map = {v: k for k, v in RBDFeatures.__dict__.iteritems() if k.startswith("RBD")}

        return ",".join([name for byte, name in bytes_map.iteritems() if byte & features])


def which(program):
    """http://stackoverflow.com/questions/377017/test-if-executable-exists-in-python"""

    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    return None


# noinspection PyArgumentList
class Ceph(object):
    # def __new__(cls, *args, **kwargs):
    #     # Singleton
    #     if not hasattr(cls, 'instance'):
    #         cls.instance = super(Ceph, cls).__new__(cls)
    #     return cls.instance

    # noinspection PyMethodParameters
    def convert_to_str(f):
        """
        Explicitelly convert all unicode strings to their :str counterpart
        """

        # noinspection PyCallingNonCallable
        def inner(*args, **kwargs):
            new_args = []
            new_kwargs = {}
            for arg in args:
                if isinstance(arg, unicode):
                    new_args.append(str(arg))
                else:
                    new_args.append(arg)

            for k, v in kwargs.iteritems():
                if isinstance(v, unicode):
                    new_kwargs[k] = str(v)
                else:
                    new_kwargs[k] = v

            return f(*new_args, **new_kwargs)

        return inner

    @convert_to_str
    def __init__(self, pool='rbd', conffile='/etc/ceph/ceph.conf', cluster='ceph'):
        """
        Init
        :param pool: Ceph cluster pool
        :type pool: str
        :param conffile: Path to ceph.conf file
        :type conffile: str
        """
        self.pool = str(pool)
        self.cluster = Rados(conffile=conffile, clustername=cluster)
        try:
            self.cluster.connect()
            self.ioctx = self.cluster.open_ioctx(self.pool)
            self.rbd = rbd.RBD()
        except ObjectNotFound:
            logger.exception("Error in ceph.py")
            raise PoolNotFound("No pool %s found" % self.pool)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cluster.shutdown()

    @convert_to_str
    def get_image_stat(self, image_name):
        if self.is_image_exists(image_name):
            return rbd.Image(self.ioctx, image_name).stat()
        return None

    def get_images_list(self, with_ext_info=False):
        """
        Get cluster images
        :param with_ext_info:
        :type with_ext_info:
        :return: list of cluster images
        :rtype: list or dict
        """
        ext_result = {}
        try:
            result = self.rbd.list(self.ioctx)
            if with_ext_info:
                for image_name in result:
                    with rbd.Image(self.ioctx, image_name) as image:
                        stat = image.stat()
                        ext_result[image_name] = stat
                        ext_result[image_name].update({"features": RBDFeatures.parse_features(image.features())})
            return result if not ext_result else ext_result
        except rbd.Error as e:
            raise e

    @convert_to_str
    def create_snapshot(self, image_name, snap_name):
        """
        Create snapshot in cluster
        :param image_name: name of image in cluster
        :type image_name: str
        :param snap_name: name of snapshot
        :type snap_name: str
        :return: status of the operation
        :rtype: bool
        """

        with rbd.Image(self.ioctx, image_name) as image:
            try:
                image.create_snap(snap_name)
                return True
            except rbd.ImageExists:
                logger.exception()
                logger.warn("Image %s exists in cluster!", snap_name)
                return False

    @convert_to_str
    def create_dump(self, image_name, snap_name, directory, pool="rbd", speed_limit=None):
        """
        Create dump of cluster image in filesystem
        :param speed_limit: limit writing speed to provided value(MB)
        :type speed_limit: int
        :param image_name: name of image in cluster
        :type image_name: str
        :param snap_name: name of already existed snapshot
        :type snap_name: str
        :param directory: directory to store result file
        :type directory: str
        :param pool: pool in rbd (defaults to "rbd")
        :type pool: str
        :return: status of the operation
        :rtype: bool
        """
        if speed_limit == 0:
            speed_limit = None

        def gen_cmd():
            if speed_limit and which('pv'):
                return "rbd export {pool}/{image}@{snap} - | pv -L {speed_limit}m > {path}"
            elif speed_limit and not which('pv'):
                logger.warn("Limiting write speed only possible with use of \"pv\", "
                            "but pv executable is not found in your PATH")
            return "rbd export {pool}/{image}@{snap} {path}"

        if not pool and self.pool:
            pool = self.pool

        with rbd.Image(self.ioctx, image_name) as image:
            try:
                if not image.is_protected_snap(snap_name):
                    image.protect_snap(snap_name)
            except (IOError, rbd.ImageNotFound):
                logger.exception("Error while checking image existance")
                return False

            computed_path = os.path.join(directory, snap_name)
            data = gen_cmd().format(
                pool=pool, image=image_name, snap=snap_name, path=computed_path, speed_limit=speed_limit
            )
            try:
                check_output(data, stderr=STDOUT, shell=True)
            except CalledProcessError:
                logger.exception("RBD call error")
                return False
            finally:
                try:
                    if image.is_protected_snap(snap_name):
                        image.unprotect_snap(snap_name)
                except (rbd.ImageNotFound, IOError):
                    logger.exception("Error while unprotecting snapshot")
            return True

    # noinspection PyPep8Naming
    @convert_to_str
    def create_dump_native(self, image_name, snap_name, fn, speed_limit=None, cb=None, bs=None):
        # TODO: Implement threaded ring-buffer or look for an aio PR https://github.com/ceph/ceph/pull/9292
        CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL = 0x8
        CEPH_OSD_OP_FLAG_FADVISE_NOCACHE = 0x40
        fadvice_flags = CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL | CEPH_OSD_OP_FLAG_FADVISE_NOCACHE
        import sys
        from time import sleep
        from tokenbucket import TokenBucket

        def sizeof_fmt(num, suffix='B'):
            for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
                if abs(num) < 1024.0:
                    return "%3.1f%s%s" % (num, unit, suffix)
                num /= 1024.0
            return "%.1f%s%s" % (num, 'Yi', suffix)

        def print_progress(cur, total):
            progress = cur / total * 100
            if not progress == 100:
                sys.stdout.write('\rProgress: %d%%(%s/%s)...' % (progress, sizeof_fmt(cur), sizeof_fmt(total)))
            else:
                sys.stdout.write("\r" + " " * 50 + "\rProgress: %d%%...done.\n" % progress)
            sys.stdout.flush()

        if cb and callable(cb):
            print_progress = cb

        if speed_limit:
            limit = int(speed_limit)  # 1024 ** 2 * 10  # 1mbps
            bucket = TokenBucket(limit, limit)
        else:
            bucket = None
        if not self.is_image_exists(image_name):
            logger.warn("No image %s exists in cluster", image_name)
            return False
        if not self.is_snapshot_exists(image_name, snap_name):
            logger.warn("No snapshot %s for image %s exists in cluster", snap_name, image_name)
            return False
        with rbd.Image(self.ioctx, image_name, snapshot=snap_name) as image, open(fn, "wb") as fd:
            if not bs:
                bs = int(1 << image.stat()['order'] * image.stripe_count())
            total = image.stat()['size']
            cur = 0
            while cur < total:
                if bucket:
                    if bucket.consume(bs):
                        try:
                            fd.write(image.read(cur, bs, fadvice_flags))
                        except rbd.InvalidArgument:
                            bs = total % bs
                            print("cur, total: %d, %d" % (cur, total))
                            fd.write(image.read(cur, bs, fadvice_flags))

                        cur += bs
                        print_progress(cur, total)

                    else:
                        sleep(1)
                else:
                    try:
                        fd.write(image.read(cur, bs, fadvice_flags))

                    except rbd.InvalidArgument:
                        print("cur, total: %d, %d" % (cur, total))
                        break
                    cur += bs
                    print_progress(cur, total)
            return True

    @convert_to_str
    def remove_snapshot(self, image_name, snap_name, force=False):
        """
        Tries to remove given snapshot from cluster
        If :force specified it will remove protected snapshots anyway
        :param image_name: name of image in cluster
        :type image_name: str
        :param snap_name: name of already existed snapshot
        :type snap_name: str
        :param force: if specified removes protected snapshot
        :type force: bool
        :return: status of the operation
        :rtype: bool
        """

        with rbd.Image(self.ioctx, image_name) as image:
            try:
                if image.is_protected_snap(snap_name):
                    if force:
                        image.unprotect_snap(snap_name)
                        self.remove_snapshot(image_name, snap_name, force=False)
                        return True
                    else:
                        return False
                else:
                    image.remove_snap(snap_name)
                    return True
            except (IOError, rbd.ImageNotFound, rbd.ImageBusy):
                logger.debug("Handled exception", exc_info=True)
                return False

    @convert_to_str
    def list_snapshots(self, image_name):
        """
        List snapshots for image
        :param image_name: name of image in cluster
        :type image_name: str
        :return: List of snapshots
        :rtype: list
        """
        if self.is_image_exists(image_name):
            with rbd.Image(self.ioctx, str(image_name)) as image:
                return image.list_snaps() or []
        else:
            return []

    @convert_to_str
    def is_image_exists(self, image_name):
        try:
            rbd.Image(self.ioctx, image_name)
            return True
        except rbd.ImageNotFound:
            return False

    @convert_to_str
    def protect(self, image_name, snap_name):
        if self.is_image_exists(image_name) and self.is_snapshot_exists(image_name, snap_name):
            with rbd.Image(self.ioctx, image_name) as image:
                if not image.features() & RBDFeatures.RBD_FEATURE_LAYERING:
                    logger.warn("Image %s doesn't support protection! Please consider enabling layering.", image_name)
                    return False
                if not image.is_protected_snap(snap_name):
                    image.protect_snap(snap_name)
                    return True
                else:
                    logger.info("Snapshot %s@%s is already protected", image_name, snap_name)
                    return False
        return False

    @convert_to_str
    def unprotect(self, image_name, snap_name):
        if self.is_image_exists(image_name) and self.is_snapshot_exists(image_name, snap_name):
            with rbd.Image(self.ioctx, image_name) as image:
                if image.is_protected_snap(snap_name):
                    image.unprotect_snap(snap_name)
                    return True
                else:
                    logger.info("Snapshot %s@%s is already unprotected", image_name, snap_name)
                    return False
        return False

    @convert_to_str
    def is_snapshot_exists(self, image_name, snap_name):
        """
        Check if snapshot exists in cluster
        :param image_name:  name of image in cluster
        :type image_name: str
        :param snap_name: name of already existed snapshot
        :type snap_name: str
        :return: True if snapshot exists, otherwise False
        :rtype: bool
        """
        if self.is_image_exists(image_name):
            with rbd.Image(self.ioctx, image_name) as image:
                try:
                    for snap in image.list_snaps():
                        if snap_name in snap['name']:
                            return True

                    return False
                except (IOError, rbd.ImageNotFound):
                    logger.exception()
                    return False
        return False
